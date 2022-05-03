#pragma once

#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "memory/memory_resource_manager.hpp"
#include "memory/umap_jemalloc_memory_resource.hpp"
#include "memory/jemalloc_memory_resource.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/encoding_type.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "resolve_type.hpp"

#include <benchmark/benchmark.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <nlohmann/json.hpp>
#include <chrono>
#include <vector>
#include <algorithm>
#include <ctime>

namespace opossum
{
    using namespace opossum;                        // NOLINT
    using namespace opossum::expression_functional; // NOLINT

    using SegmentLocations = std::unordered_map<std::tuple<std::string, ChunkID, ColumnID>, std::string, segment_key_hash>;

    int move_segment_to_device(std::shared_ptr<Table> table, const std::string &table_name, ChunkID chunk_id, const ColumnID column_id, const std::string &device_name, SegmentLocations &new_segment_locations)
    {
        const auto segment_key = std::make_tuple(table_name, chunk_id, column_id);
        new_segment_locations.emplace(segment_key, device_name);
        if (TieringSelectionPlugin::segment_locations.contains(segment_key))
        {
            if (TieringSelectionPlugin::segment_locations.at(segment_key) == device_name)
            {
                std::cout << "Segment is already on the correct device. Skipping." << std::endl;
                return 0;
            }
        }
        else
        {
            // do not continue and rather copy the segment to dram again if it was initialized there
            // but now use the allocator that we use
            // continue;
        }

        std::cout << "Moving Segment " << table_name << " " << chunk_id << " " << column_id << " to " << device_name << std::endl;

        auto resource = MemoryResourceManager::get().get_memory_resource_for_device(device_name);
        const auto allocator = PolymorphicAllocator<void>{resource};
        const auto &target_segment = table->get_chunk(chunk_id)->get_segment(column_id);
        const auto migrated_segment = target_segment->copy_using_allocator(allocator);
        table->get_chunk(chunk_id)->replace_segment(column_id, migrated_segment);
        return 1;
    }

    std::map<std::string, std::shared_ptr<TableWrapper>> create_table_wrappers(StorageManager &sm)
    {
        std::map<std::string, std::shared_ptr<TableWrapper>> wrapper_map;
        for (const auto &table_name : sm.table_names())
        {

            auto table = sm.get_table(table_name);
            auto table_wrapper = std::make_shared<TableWrapper>(table);
            table_wrapper->execute();
            table_wrapper->never_clear_output();

            wrapper_map.emplace(table_name, table_wrapper);
        }

        return wrapper_map;
    }

    void generate_data(float scale_factor)
    {
        const auto default_encoding = EncodingType::Dictionary;

        auto benchmark_config = BenchmarkConfig::get_default_config();
        // TODO(anyone): setup benchmark_config with the given default_encoding
        // benchmark_config.encoding_config = EncodingConfig{SegmentEncodingSpec{default_encoding}};

        auto &sm = Hyrise::get().storage_manager;

        if (!sm.has_table("lineitem"))
        {
            std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and " << default_encoding
                      << " encoding:" << std::endl;
            TPCHTableGenerator(scale_factor, ClusteringConfiguration::None,
                               std::make_shared<BenchmarkConfig>(benchmark_config))
                .generate_and_store();
        }
    }

    void generate_random_data_for_devices(const std::vector<std::string> &devices, std::vector<pmr_vector<uint32_t>> &random_data_per_device, int random_data_size_mb)
    {
        std::srand(unsigned(std::time(nullptr)));
        for (const auto &device_name : devices)
        {
            if (device_name == "DRAM")
            {
                continue;
            }
            auto resource = MemoryResourceManager::get().get_memory_resource_for_device(device_name);
            const auto allocator = PolymorphicAllocator<void>{resource};
            random_data_per_device.emplace_back(random_data_size_mb * 1024 * 1024 / sizeof(uint32_t), allocator); // size of umap buffer

            std::generate(random_data_per_device.back().begin(), random_data_per_device.back().end(), std::rand);
        }
    }

    void clear_caches(const std::vector<pmr_vector<uint32_t>> &random_data_per_device)
    {
        uint32_t random_data_sum;
        for (const auto &random_data : random_data_per_device)
        {
            benchmark::DoNotOptimize(random_data_sum = std::accumulate(random_data.begin(), random_data.end(), 0));
            benchmark::ClobberMemory();
        }
    }

    void move_segments_to_device(const std::string &device_name, const std::string &table_name, std::shared_ptr<Table> table, ColumnID column_id)
    {
        SegmentLocations new_segment_locations = {};
        // move all table segments to device
        int moved_segment_count = 0;
        for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
        {
            moved_segment_count += move_segment_to_device(table, table_name, chunk_id, column_id, device_name, new_segment_locations);
        }
        TieringSelectionPlugin::segment_locations = std::move(new_segment_locations);
        std::cout << "moved " << moved_segment_count << " segments to device (rest was already there)" << std::endl;
    }

    void get_reference_segments_with_poslist_for_access_pattern(const std::string &access_pattern, ColumnID column_id, std::shared_ptr<Table> table, int monotonic_access_stride, std::vector<std::shared_ptr<ReferenceSegment>> &reference_segments)
    {

        if (access_pattern == "random_multiple_chunk")
        {
            std::vector<RowID> all_positions = {};
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            {
                auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

                for (auto i = ChunkOffset{0}; i < segment->size(); ++i)
                {
                    all_positions.push_back(RowID{chunk_id, i});
                }
            }

            std::random_shuffle(all_positions.begin(), all_positions.end());

            size_t j = 0;
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            {
                auto pos_list = std::make_shared<RowIDPosList>();
                auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

                for (auto i = ChunkOffset{0}; i < segment->size(); ++i)
                {
                    pos_list->push_back(RowID{all_positions[j].chunk_id, all_positions[j].chunk_offset});
                    j += 1;
                }
                reference_segments.push_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            }
        }
        else
        {
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            {
                auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

                if (access_pattern == "sequential")
                {
                    // todo EntireChunkPoslist
                    // pos_list->guarantee_single_chunk();
                    // for (auto i = ChunkOffset{0}; i < segment->size(); ++i)
                    // {
                    //     pos_list->push_back(RowID{chunk_id, i});
                    // }
                    auto entire_pos_list = std::make_shared<EntireChunkPosList>(chunk_id, segment->size());
                    reference_segments.push_back(std::make_shared<ReferenceSegment>(table, column_id, entire_pos_list));
                    continue;
                }

                auto pos_list = std::make_shared<RowIDPosList>();

                if (access_pattern == "random_single_chunk")
                {
                    pos_list->guarantee_single_chunk();
                    for (auto i = ChunkOffset{0}; i < segment->size(); ++i)
                    {
                        pos_list->push_back(RowID{chunk_id, i});
                    }
                    std::random_shuffle(pos_list->begin(), pos_list->end());
                }
                else if (access_pattern == "monotonic")
                {
                    pos_list->guarantee_single_chunk();
                    // stride could also be std::rand() % (2 * monotonic_access_stride + 1)
                    for (auto i = ChunkOffset{0}; i < segment->size(); i += monotonic_access_stride)
                    {
                        pos_list->push_back(RowID{chunk_id, i});
                    }
                }
                else if (access_pattern == "single_point")
                {
                    pos_list->guarantee_single_chunk();
                    pos_list->push_back(RowID{chunk_id, std::rand() % segment->size()});
                }
                else
                {
                    throw std::runtime_error("Unknown access pattern");
                }

                reference_segments.push_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            }
        }
    }

    size_t get_num_tuples_per_iteration(const std::vector<std::shared_ptr<ReferenceSegment>> &reference_segments)
    {
        size_t num_tuples_scanned_per_iteration = 0;
        for (const auto &segment : reference_segments)
        {
            num_tuples_scanned_per_iteration += segment->size();
        }
        return num_tuples_scanned_per_iteration;
    }

    // this is to speed up google benchmark (otherwise it does too many iterations!)
    // this is an issue because we are doing a lot of additional work during each iteration (cache clearing + moving segments), which isn't counter
    // and that leads to google benchmark running a single benchmark for 2+ hours.
    // this is especially an issue for short-running benchmarks (e.g. single_point as benchmark wants to repeat them very often)
    // We have to remember to divide by this again in python.
    int get_access_pattern_runtime_multiplicator_for_g_benchmark(const std::string &access_pattern)
    {
        if (access_pattern == "single_point")
        {
            return 100; // remember to divide by this again,
        }
        else if (access_pattern == "monotonic")
        {
            return 5;
        }

        return 1;
    }

    void register_benchmarks(
        const std::vector<std::string> &devices,
        ColumnID column_id,
        std::shared_ptr<Table> table,
        int monotonic_access_stride,
        int benchmark_min_time_seconds,
        std::function<void(benchmark::State &,
                           const std::string &,
                           const std::string &,
                           const std::vector<std::shared_ptr<ReferenceSegment>> &,
                           int)>
            TieringCalibrationSegmentAccess)
    {
        const std::vector<std::string> access_patterns = {
            "random_single_chunk",
            "sequential",
            "random_multiple_chunk",
            "monotonic",
            "single_point"};
        for (auto &device_name : devices)
        {
            for (const auto &access_pattern : access_patterns)
            {
                std::cout << "Registering benchmark for device: " << device_name << " with access pattern: " << access_pattern << std::endl;

                std::vector<std::shared_ptr<ReferenceSegment>> reference_segments = {};
                get_reference_segments_with_poslist_for_access_pattern(access_pattern, column_id, table, monotonic_access_stride, reference_segments);
                auto num_tuples_scanned_per_iteration = get_num_tuples_per_iteration(reference_segments);
                auto runtime_multiplier = get_access_pattern_runtime_multiplicator_for_g_benchmark(access_pattern);

                // benchmark::RegisterBenchmark(("TieringCalibrationTableScan " + access_pattern + " " + device_name).c_str(), TieringCalibrationTableScan, device_name, access_pattern);

                std::stringstream ss;
                ss << "TieringCalibrationSegmentAccess;";
                ss << access_pattern << ";";
                ss << device_name << ";";
                ss << std::to_string(num_tuples_scanned_per_iteration) << ";";
                ss << std::to_string(runtime_multiplier) << ";";

                auto bm = benchmark::RegisterBenchmark(ss.str().c_str(), TieringCalibrationSegmentAccess, device_name, access_pattern, reference_segments, runtime_multiplier);
                bm->UseManualTime();
                bm->MinTime(benchmark_min_time_seconds); // max wallclock time should be 5 * mintime
            }
        }
    }

    // scale factor should be sufficient size so we don't just measure the caches
    void tiering_calibration(const std::string &file_path, const std::vector<std::string> &devices, const float scale_factor, const float benchmark_min_time_seconds, const int random_data_size_per_device_mb, const int monotonic_access_stride)
    {
        std::cout << "Tiering calibration from plugin" << std::endl;
        const auto table_name = "lineitem";
        const auto column_id = ColumnID{6};

        generate_data(scale_factor);

        auto &sm = Hyrise::get().storage_manager;
        std::map<std::string, std::shared_ptr<TableWrapper>> _table_wrapper_map = create_table_wrappers(sm);
        auto table = sm.get_table(table_name);
        auto datatype = table->column_data_type(column_id);
        auto table_wrapper = _table_wrapper_map.at(table_name);

        std::cout << "datatype for column 6: " << datatype << std::endl;

        std::vector<pmr_vector<uint32_t>> random_data_per_device = {};
        generate_random_data_for_devices(devices, random_data_per_device, random_data_size_per_device_mb);

        auto TieringCalibrationSegmentAccess = [&](benchmark::State &state, const std::string &device_name, const std::string &access_pattern, const std::vector<std::shared_ptr<ReferenceSegment>> &reference_segments, int runtime_multiplier)
        {
            std::cout << "device_name: " << device_name << " access_pattern: " << access_pattern << std::endl;
            move_segments_to_device(device_name, table_name, table, column_id);

            /**
             * Measurement: for all segments, access all indices in the poslist
             * Let n be the number of rows over all segments for the given column.
             * - sequential: n
             * - random: n
             * - monotonic: n / monotonic_access_stride -> do this "monotonic_access_stride" times
             * - single_point: # chunks --> do this # rows in chunk times
             */

            for (auto _ : state)
            {
                // std::cout << "benchmark iteration start" << std::endl;

                clear_caches(random_data_per_device);

                auto start = std::chrono::high_resolution_clock::now();

                for (const auto &segment : reference_segments)
                {
                    ReferenceSegmentIterable<float, EraseReferencedSegmentType::No> reference_segment_iterable(*segment);
                    reference_segment_iterable.with_iterators([](auto it, auto end)
                                                              {

                    for (; it != end; ++it)
                    {
                        double val;
                        benchmark::DoNotOptimize(val = it->value());
                        benchmark::ClobberMemory();
                    } });
                }

                auto end = std::chrono::high_resolution_clock::now();

                auto elapsed_seconds =
                    std::chrono::duration_cast<std::chrono::duration<double>>(
                        end - start);

                state.SetIterationTime(elapsed_seconds.count() * runtime_multiplier);
            }
        };

        // todo(ben): MAYBE measure both artificial segment and table scan
        register_benchmarks(devices, column_id, table, monotonic_access_stride, benchmark_min_time_seconds, TieringCalibrationSegmentAccess);

        std::vector<std::string> arguments = {"TieringSelectionPlugin", "--benchmark_out=" + file_path, "--benchmark_out_format=json"};
        std::vector<char *> argv;
        for (const auto &arg : arguments)
            argv.push_back((char *)arg.data());
        argv.push_back(nullptr);

        // don't count nullptr at the end
        int argc = argv.size() - 1;
        benchmark::Initialize(&argc, argv.data());
        benchmark::RunSpecifiedBenchmarks();
        benchmark::Shutdown();
    }
}
