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
#include <thread>
#include <iostream>

namespace opossum
{
    std::atomic_bool stop_threads = false;
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
                // std::cout << "Segment is already on the correct device. Skipping." << std::endl;
                return 0;
            }
        }

        // do not continue and rather copy the segment to dram again if it was initialized there
        // but now use the allocator that we use

        // std::cout << "Moving Segment " << table_name << " " << chunk_id << " " << column_id << " to " << device_name << std::endl;

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
            // if (device_name == "DRAM")
            // {
            //     continue;
            // }
            auto resource = MemoryResourceManager::get().get_memory_resource_for_device(device_name);
            const auto allocator = PolymorphicAllocator<void>{resource};
            random_data_per_device.emplace_back(random_data_size_mb * 1024 * 1024 / sizeof(uint32_t), allocator); // size of umap buffer

            std::generate(random_data_per_device.back().begin(), random_data_per_device.back().end(), std::rand);
        }
    }

    void clear_caches(const std::vector<pmr_vector<uint32_t>> &random_data_per_device)
    {
        std::cout << "Clearing caches" << std::endl;
        uint32_t random_data_sum;
        std::vector<std::thread> threads = {};
        for (int j = 0; j < 32; j++)
        {
            threads.emplace_back([&]()
                                 {
                for (int i = 0; i < 5; i++)
                {
                    for (const auto &random_data : random_data_per_device)
                    {
                        benchmark::DoNotOptimize(random_data_sum = std::accumulate(random_data.begin(), random_data.end(), 0));
                        benchmark::ClobberMemory();
                    }
                } });
        }
        for (auto &thread : threads)
        {
            thread.join();
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
            std::vector<std::vector<RowID>> row_ids_for_chunks = {};
            // for each segment shuffle one poslist
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            {
                std::vector<RowID> row_ids_for_chunk = {};

                for (auto i = ChunkOffset{0}; i < table->get_chunk(chunk_id)->get_segment(column_id)->size(); i++)
                {
                    row_ids_for_chunk.push_back(RowID{chunk_id, i});
                }

                std::random_shuffle(row_ids_for_chunk.begin(), row_ids_for_chunk.end());
                row_ids_for_chunks.push_back(row_ids_for_chunk);
            }

            unsigned int num_values = table->chunk_count() * (table->get_chunk(ChunkID{0})->get_segment(column_id)->size() / 10000);
            auto pos_list = std::make_shared<RowIDPosList>();
            for (unsigned int i = 0; i < num_values; ++i) {
                auto row_ids_for_chunk = row_ids_for_chunks[i % row_ids_for_chunks.size()];
                auto row_id = row_ids_for_chunk[i % row_ids_for_chunk.size()];
                pos_list->push_back(RowID{row_id.chunk_id, row_id.chunk_offset});
                std::cout << "adding chunk_id " << row_id.chunk_id << " chunk_offset " << row_id.chunk_offset << std::endl;
            }
            reference_segments.push_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));


            // for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            // {
            //     auto pos_list = std::make_shared<RowIDPosList>();
            //     for (auto i = ChunkOffset{0}; i < table->get_chunk(chunk_id)->get_segment(column_id)->size() / 1000; i++)
            //     {
            //         // for each tuple (with stride though) get a random position. Make sure that they two subsequent tuples don't go to the same segment.
            //         auto positions = all_positions_per_chunk[i % all_positions_per_chunk.size()];
            //         auto position = positions[i % positions.size()];
            //         pos_list->push_back(RowID{position.chunk_id, position.chunk_offset});
            //         if (position.chunk_id > table->chunk_count())
            //         {
            //             std::cout << "adding chunk_id " << position.chunk_id << " chunk_offset " << position.chunk_offset << std::endl;
            //         }
            //     }
            //     reference_segments.push_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            // }
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
    int get_access_pattern_runtime_multiplicator_for_g_benchmark(const std::string &access_pattern, const std::string &device_name, const std::string &datatype)
    {
        int multiplicator = 1;
        if (access_pattern == "single_point")
        {
            multiplicator = 100; // remember to divide by this again,
        }
        else if (access_pattern == "monotonic")
        {
            multiplicator = 5;
        }
        else if (access_pattern == "random_multiple_chunk")
        {
            multiplicator = 100;
        }
        else if (access_pattern == "random_single_chunk")
        {
            multiplicator = 100;
        }
        else if (access_pattern == "sequential")
        {
            multiplicator = 5;
        }

        // if (device_name != "DRAM")
        // {
        //     multiplicator *= 10;
        // }
        if (datatype == "float")
        {
            multiplicator *= 100;
        }
        if (datatype == "string")
        {
            multiplicator *= 100;
        }

        return multiplicator;
    }

    void register_benchmarks(
        const std::vector<std::string> &devices,
        ColumnID column_id,
        std::shared_ptr<Table> table,
        int monotonic_access_stride,
        int benchmark_min_time_seconds,
        float reference_segments_size_percentage,
        std::vector<std::string> access_patterns,
        std::function<void(benchmark::State &,
                           const std::string &,
                           const std::string &,
                           const std::vector<std::shared_ptr<ReferenceSegment>> &,
                           const std::vector<std::shared_ptr<ReferenceSegment>> &,
                           int,
                           ColumnID)>
            TieringCalibrationSegmentAccess)
    {

        auto datatype = table->column_data_type(column_id);
        std::stringstream ss;
        ss << datatype;
        auto datatype_string = ss.str();

        std::cout << "datatype for column "
                  << column_id << " : " << datatype_string << std::endl;

        for (auto &device_name : devices)
        {
            for (const auto &access_pattern : access_patterns)
            {
                std::cout << "Registering benchmark for device: " << device_name << " with access pattern: " << access_pattern << " and datatype: " << datatype_string << std::endl;

                std::vector<std::shared_ptr<ReferenceSegment>> reference_segments = {};
                get_reference_segments_with_poslist_for_access_pattern(access_pattern, column_id, table, monotonic_access_stride, reference_segments);

                std::vector<std::shared_ptr<ReferenceSegment>> reference_segments_resized = {};
                std::copy_n(reference_segments.begin(), std::max(1, static_cast<int>(reference_segments.size() * reference_segments_size_percentage)), std::back_inserter(reference_segments_resized));

                std::vector<std::shared_ptr<ReferenceSegment>> concurrent_threads_reference_segments = {};
                get_reference_segments_with_poslist_for_access_pattern("sequential", ColumnID{5}, table, monotonic_access_stride, concurrent_threads_reference_segments);

                auto num_tuples_scanned_per_iteration = get_num_tuples_per_iteration(reference_segments_resized);
                auto runtime_multiplier = get_access_pattern_runtime_multiplicator_for_g_benchmark(access_pattern, device_name, datatype_string);

                // benchmark::RegisterBenchmark(("TieringCalibrationTableScan " + access_pattern + " " + device_name).c_str(), TieringCalibrationTableScan, device_name, access_pattern);

                int bytes_per_value;
                if (datatype_string == "string")
                {
                    std::vector<std::shared_ptr<ReferenceSegment>> seq_reference_segments = {};
                    get_reference_segments_with_poslist_for_access_pattern("sequential", column_id, table, monotonic_access_stride, seq_reference_segments);

                    ReferenceSegmentIterable<opossum::pmr_string, EraseReferencedSegmentType::No> reference_segment_iterable(*seq_reference_segments[0]);
                    reference_segment_iterable.with_iterators([&](auto it, auto end)
                                                              {
                        int i = 0;
                        bytes_per_value = 0;
                        for (; it != end; ++it)
                        {
                            i++;
                            bytes_per_value += it->value().size();
                        }
                        bytes_per_value /= i; });
                }
                else
                {
                    bytes_per_value = sizeof(float);
                }

                std::stringstream ss;
                ss << "TieringCalibrationSegmentAccess;";
                ss << access_pattern << ";";
                ss << device_name << ";";
                ss << std::to_string(num_tuples_scanned_per_iteration) << ";";
                ss << std::to_string(runtime_multiplier) << ";";
                ss << datatype_string << ";";
                ss << bytes_per_value << ";";

                std::cout << ss.str() << std::endl;

                auto bm = benchmark::RegisterBenchmark(ss.str().c_str(), TieringCalibrationSegmentAccess, device_name, access_pattern, reference_segments_resized, concurrent_threads_reference_segments, runtime_multiplier, column_id);
                bm->UseManualTime();
                bm->MinTime(benchmark_min_time_seconds); // max wallclock time should be 5 * mintime
            }
        }
    }

    // scale factor should be sufficient size so we don't just measure the caches
    void tiering_calibration(const std::string &file_path, const std::vector<std::string> &devices, const float scale_factor, const float benchmark_min_time_seconds, const int random_data_size_per_device_mb, const int monotonic_access_stride, const int num_concurrent_threads, const int num_reader_threads, std::string modes, std::string access_patterns_string)
    {
        std::cout << "Tiering calibration from plugin" << std::endl;
        const auto table_name = "lineitem";

        generate_data(scale_factor);

        auto &sm = Hyrise::get().storage_manager;
        std::map<std::string, std::shared_ptr<TableWrapper>> _table_wrapper_map = create_table_wrappers(sm);
        auto table = sm.get_table(table_name);

        std::vector<pmr_vector<uint32_t>>
            random_data_per_device = {};
        generate_random_data_for_devices(devices, random_data_per_device, random_data_size_per_device_mb);

        auto TieringCalibrationSegmentAccess = [&](benchmark::State &state, const std::string &device_name, const std::string &access_pattern, const std::vector<std::shared_ptr<ReferenceSegment>> &reference_segments, const std::vector<std::shared_ptr<ReferenceSegment>> &concurrent_thread_reference_segments, int runtime_multiplier, ColumnID column_id)
        {
            std::cout << "device_name: " << device_name << " access_pattern: " << access_pattern << " column id: " << column_id << std::endl;
            move_segments_to_device(device_name, table_name, table, column_id);

            auto num_reader_threads_for_access_pattern = num_reader_threads;
            if (access_pattern == "random_single_chunk" || access_pattern == "random_multiple_chunk") {
                num_reader_threads_for_access_pattern = 1;
            }

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

                std::cout << "clear caches finished" << std::endl;

                std::vector<std::thread> concurrent_threads;
                for (int thread_id = 0; thread_id < num_concurrent_threads; thread_id++)
                {
                    concurrent_threads.push_back(std::thread([=]()
                                                             {
                        // std::stringstream ss;
                        // ss << "thread_id: " << thread_id << " started \n";
                        // std::cout << ss.str();
                        while (!stop_threads) {
                            for (auto it = concurrent_thread_reference_segments.rbegin(); it != concurrent_thread_reference_segments.rend(); ++it)
                            {
                                const auto &segment = *it;
                                if (stop_threads) {
                                    // std::cout << "thread " << thread_id << " finished\n";
                                    return;
                                }
                                // std::cout << "segment: " << i << std::endl;
                                // resolve segment type was a hassle

                                resolve_data_type(segment->data_type(), [&](auto type)
                                                    { using SegmentDataType = typename decltype(type)::type;

                                        ReferenceSegmentIterable<SegmentDataType, EraseReferencedSegmentType::No> reference_segment_iterable(*segment);
                                        reference_segment_iterable.with_iterators([](auto it, auto end) {
                                                SegmentDataType val;
                                                for (; it != end; ++it) {
                                                    benchmark::DoNotOptimize(val = it->value());
                                                    benchmark::ClobberMemory();
                                                }
                                            }
                                        );
                                });
                            }
                        }
                        // std::cout << "thread " << thread_id << " finished\n";
                        return; }));
                }

                // int i = 0;

                std::vector<std::thread> reader_threads;
                std::vector<std::vector<std::shared_ptr<ReferenceSegment>>> reference_segments_per_reader_thread = {};
                size_t num_reference_segments_per_reader_thread = reference_segments.size() / num_reader_threads_for_access_pattern;
                for (size_t i = 0; i < reference_segments.size(); i += num_reference_segments_per_reader_thread)
                {
                    auto last = std::min(reference_segments.size(), i + num_reference_segments_per_reader_thread);
                    reference_segments_per_reader_thread.emplace_back(reference_segments.begin() + i, reference_segments.begin() + last);
                }

                auto start = std::chrono::high_resolution_clock::now();

                // std::cout << "num_reader_threads_for_access_pattern" << num_reader_threads_for_access_pattern << std::endl;
                // std::cout << "num_concurrent_threads" << num_concurrent_threads << std::endl;
                // std::cout << "reference_segments_per_reader_thread" << reference_segments_per_reader_thread.size() << std::endl;
                // std::cout << "reference_segments.size()" << reference_segments.size() << std::endl;
                // std::cout << "reference_segments_per_reader_thread[0]" << reference_segments_per_reader_thread[0].size() << std::endl;

                for (int thread_id = 0; thread_id < num_reader_threads_for_access_pattern; thread_id++)
                {
                    // std::cout << "starting thread" << thread_id << std::endl;
                    reader_threads.push_back(std::thread([=]()
                                                         {
                        for (const auto &segment : reference_segments_per_reader_thread[thread_id])
                        {
                            // std::cout << "reading segment\n";
                            resolve_data_type(segment->data_type(), [&](auto type)
                                              {
                                // auto abc = 0;
                                using SegmentDataType = typename decltype(type)::type;
                                ReferenceSegmentIterable<SegmentDataType, EraseReferencedSegmentType::No> reference_segment_iterable(*segment);
                                reference_segment_iterable.with_iterators([&](auto it, auto end)
                                                                          {
                                    SegmentDataType val;
                                    for (; it != end; ++it)
                                    {
                                        // abc++;
                                        benchmark::DoNotOptimize(val = it->value());
                                        // benchmark::DoNotOptimize(val = val + val);
                                        benchmark::ClobberMemory();

                                        // std::cout << abc << std::endl;
                                    } }); });
                        }
                        return; }));
                }
                for (auto &reader_thread : reader_threads)
                {
                    reader_thread.join();
                }
                auto end = std::chrono::high_resolution_clock::now();

                stop_threads = true;
                for (auto &concurrent_thread : concurrent_threads)
                {
                    concurrent_thread.join();
                }
                stop_threads = false;

                auto elapsed_seconds =
                    std::chrono::duration_cast<std::chrono::duration<double>>(
                        end - start);

                // std::cout << "benchmark iteration finished, duration (without multiplier) was " << elapsed_seconds.count() << std::endl;
                state.SetIterationTime(elapsed_seconds.count() * runtime_multiplier);
            }
        };

        auto access_patterns = std::vector<std::string>{};
        boost::split(access_patterns, access_patterns_string, boost::is_any_of(";"), boost::token_compress_on);

        if (modes.find("INT") != std::string::npos)
        {
            register_benchmarks(devices, ColumnID{0}, table, monotonic_access_stride, benchmark_min_time_seconds, 1.0, access_patterns, TieringCalibrationSegmentAccess);
        }

        if (modes.find("FLOAT") != std::string::npos)
        {
            register_benchmarks(devices, ColumnID{6}, table, monotonic_access_stride, benchmark_min_time_seconds, 1.0, access_patterns, TieringCalibrationSegmentAccess);
        }

        if (modes.find("STRING") != std::string::npos)
        {
            register_benchmarks(devices, ColumnID{15}, table, monotonic_access_stride, benchmark_min_time_seconds, 0.1, access_patterns, TieringCalibrationSegmentAccess);
        }

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
