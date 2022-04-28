#include "tiering_selection_plugin.hpp"

#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
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

    void move_segment_to_device(std::shared_ptr<Table> table, const std::string &table_name, ChunkID chunk_id, ColumnID column_id, const std::string &device_name, SegmentLocations &new_segment_locations)
    {
        const auto segment_key = std::make_tuple(table_name, chunk_id, column_id);
        new_segment_locations.emplace(segment_key, device_name);
        if (TieringSelectionPlugin::segment_locations.contains(segment_key))
        {
            if (TieringSelectionPlugin::segment_locations.at(segment_key) == device_name)
            {
                // std::cout << "Segment is already on the correct device. Skipping." << std::endl;
                return;
            }
        }
        else
        {
            // do not continue and rather copy the segment to dram again if it was initialized there
            // but now use the allocator that we use
            // continue;
        }

        // std::cout << "Moving Segment " << table_name << " " << chunk_id << " " << column_id << " to " << device_name << std::endl;

        auto resource = MemoryResourceManager::get().get_memory_resource_for_device(device_name);
        const auto allocator = PolymorphicAllocator<void>{resource};
        const auto &target_segment = table->get_chunk(chunk_id)->get_segment(column_id);
        const auto migrated_segment = target_segment->copy_using_allocator(allocator);
        table->get_chunk(chunk_id)->replace_segment(column_id, migrated_segment);
    }

    void tiering_calibration(const std::string &file_path, std::vector<std::string> devices)
    {
        std::cout << "Tiering calibration from plugin" << std::endl;

        auto &sm = Hyrise::get().storage_manager;
        const auto scale_factor = 0.01f; // sufficient size so we don't just measure the caches
        const auto default_encoding = EncodingType::Dictionary;

        auto benchmark_config = BenchmarkConfig::get_default_config();
        // TODO(anyone): setup benchmark_config with the given default_encoding
        // benchmark_config.encoding_config = EncodingConfig{SegmentEncodingSpec{default_encoding}};

        if (!sm.has_table("lineitem"))
        {
            std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and " << default_encoding
                      << " encoding:" << std::endl;
            TPCHTableGenerator(scale_factor, ClusteringConfiguration::None,
                               std::make_shared<BenchmarkConfig>(benchmark_config))
                .generate_and_store();
        }

        std::map<std::string, std::shared_ptr<TableWrapper>> _table_wrapper_map = create_table_wrappers(sm);
        auto table_name = "lineitem";
        auto table = sm.get_table(table_name);

        auto column_id = ColumnID{6};
        auto datatype = table->column_data_type(column_id);

        std::cout << "datatype for column 6: " << datatype << std::endl;
        // Predicates as in TPC-H Q6, ordered by selectivity. Not necessarily the same order as determined by the optimizer
        std::shared_ptr<PQPColumnExpression>
            _tpchq6_discount_operand = pqp_column_(column_id, table->column_data_type(column_id),
                                                   table->column_is_nullable(column_id), "");
        std::shared_ptr<BetweenExpression> _tpchq6_discount_predicate = std::make_shared<BetweenExpression>(
            PredicateCondition::BetweenInclusive, _tpchq6_discount_operand, value_(0.05), value_(0.70001));
        auto table_wrapper = _table_wrapper_map.at(table_name);

        // auto TieringCalibrationTableScan = [&](benchmark::State &state, const auto &device_name, const auto &access_pattern)
        // {
        //     SegmentLocations new_segment_locations = {};
        //     // move all table segments to device
        //     for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
        //     {
        //         move_segment_to_device(table, table_name, chunk_id, column_id, device_name, new_segment_locations);
        //     }
        //     TieringSelectionPlugin::segment_locations = std::move(new_segment_locations);

        //     // todo access pattern

        //     for (auto _ : state)
        //     {
        //         // Todo(Ben) resolve data type, resolve segment type statt tablescan
        //         // poslisten erstellen mit reference_segment_test
        //         const auto table_scan = std::make_shared<TableScan>(table_wrapper, _tpchq6_discount_predicate);
        //         table_scan->execute();
        //     }
        // };

        const auto monotonic_access_stride = 5; // todo determine a representative value

        auto resource = MemoryResourceManager::get().get_memory_resource_for_device("hyrise-tiering/mrcl");
        const auto allocator = PolymorphicAllocator<void>{resource};
        pmr_vector<uint32_t> random_data(50 * 1024 * 1024 / sizeof(uint32_t), allocator);
        std::srand(unsigned(std::time(nullptr)));
        std::generate(random_data.begin(), random_data.end(), std::rand);

        auto TieringCalibrationSegmentAccess = [&](benchmark::State &state, const auto &device_name, const auto &access_pattern)
        {
            std::cout << "device_name: " << device_name << " access_pattern: " << access_pattern << std::endl;

            SegmentLocations new_segment_locations = {};
            // move all table segments to device
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            {
                move_segment_to_device(table, table_name, chunk_id, column_id, device_name, new_segment_locations);
            }
            TieringSelectionPlugin::segment_locations = std::move(new_segment_locations);

            std::vector<std::shared_ptr<ReferenceSegment>> reference_segments = {};
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
            {
                auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

                auto pos_list = std::make_shared<RowIDPosList>();

                if (access_pattern == "sequential")
                {
                    for (auto i = ChunkOffset{0}; i < segment->size(); ++i)
                    {
                        pos_list->push_back(RowID{chunk_id, i});
                    }
                }
                else if (access_pattern == "random")
                {
                    for (auto i = ChunkOffset{0}; i < segment->size(); ++i)
                    {
                        pos_list->push_back(RowID{chunk_id, i}); // todo(ben): what about other chunk?
                    }
                    std::random_shuffle(pos_list->begin(), pos_list->end());
                }
                else if (access_pattern == "monotonic")
                {
                    // stride could also be std::rand() % (2 * monotonic_access_stride + 1)
                    for (auto i = ChunkOffset{0}; i < segment->size(); i += monotonic_access_stride)
                    {
                        pos_list->push_back(RowID{chunk_id, i});
                    }
                }
                else if (access_pattern == "single_point")
                {
                    pos_list->push_back(RowID{chunk_id, std::rand() % segment->size()});
                }
                else
                {
                    throw std::runtime_error("Unknown access pattern");
                }

                reference_segments.push_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
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
                std::chrono::duration<double> elapsed_seconds = {};

                // todo: clear cache
                // todo: validate timer
                uint32_t random_data_sum;
                benchmark::DoNotOptimize(random_data_sum = std::accumulate(random_data.begin(), random_data.end(), 0));
                benchmark::ClobberMemory();

                auto start = std::chrono::high_resolution_clock::now();

                for (const auto &segment : reference_segments)
                {
                    ReferenceSegmentIterable<double, EraseReferencedSegmentType::No> reference_segment_iterable(*segment);
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

                auto elapsed_seconds_rep =
                    std::chrono::duration_cast<std::chrono::duration<double>>(
                        end - start);

                elapsed_seconds += elapsed_seconds_rep;
                state.SetIterationTime(elapsed_seconds.count());
            }
        };

        // todo(ben): MAYBE measure both artificial segment and table scan
        const std::vector<std::string> access_patterns = {
            "sequential",
            "random",
            "monotonic",
            "single_point"};
        for (auto &device_name : devices)
        {
            for (const auto &access_pattern : access_patterns)
            {

                auto repetitions = 1;
                if (access_pattern == "single_point")
                {
                    repetitions = table->get_chunk(ChunkID{0})->get_segment(column_id)->size();
                }
                else if (access_pattern == "monotonic")
                {
                    repetitions = monotonic_access_stride;
                }
                std::cout << "Benchmarking device: " << device_name << " with access pattern: " << access_pattern << std::endl;
                // benchmark::RegisterBenchmark(("TieringCalibrationTableScan " + access_pattern + " " + device_name).c_str(), TieringCalibrationTableScan, device_name, access_pattern);
                auto bm = benchmark::RegisterBenchmark(("TieringCalibrationSegmentAccess;" + access_pattern + ";" + device_name + ";" + std::to_string(repetitions) + ";").c_str(), TieringCalibrationSegmentAccess, device_name, access_pattern);
                bm->UseManualTime();
                // bm->MinTime(1.0);
            }
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

    void apply_tiering_configuration(const std::string &json_configuration_path, size_t task_count = 0ul)
    {
        Assert(std::filesystem::is_regular_file(json_configuration_path), "No such file: " + json_configuration_path);

        std::cout << "Starting to apply tiering configuration '" + json_configuration_path;
        if (task_count != 0)
        {
            std::cout << "Using thread count: " << task_count;
        }

        auto segment_count_pipeline = SQLPipelineBuilder{std::string{"SELECT COUNT(*) FROM meta_segments;"}}.create_pipeline();
        const auto [segment_count_pipeline_status, segment_count_table] = segment_count_pipeline.get_result_table();
        const auto segment_count = *segment_count_table->get_value<int64_t>(ColumnID{0}, 0ul);

        std::ifstream config_stream(json_configuration_path);
        nlohmann::json config;
        config_stream >> config;

        Assert(config.contains("configuration"), "Configuration dictionary missing in compression configuration JSON.");

        auto config_segment_count = std::atomic_int{0};
        auto moved_segment_count = std::atomic_int{0};
        auto &storage_manager = Hyrise::get().storage_manager;

        const auto run_start = std::chrono::steady_clock::now();
        for (const auto p : Hyrise::get().plugin_manager.loaded_plugins())
            std::cout << p << " plugin loaded." << std::endl;

        SegmentLocations new_segment_locations = {};

        for (const auto &[table_name, segment_configs] : config["configuration"].items())
        {
            const auto &table = storage_manager.get_table(table_name);
            std::cout << "Migrating segments for table: " << table_name << std::endl;

            for (const auto &[i, segment_config] : segment_configs.items())
            {
                ChunkID chunk_id = ChunkID{segment_config["chunk_id"].get<uint32_t>()};
                ColumnID column_id = ColumnID{segment_config["column_id"].get<uint16_t>()};
                std::string device_name = segment_config["device_name"];

                config_segment_count++;
                std::cout << "Segment " << (int)config_segment_count << " with table_name: " << table_name << " chunk_id: " << chunk_id << " column_id: " << column_id << " should be on device_name: " << device_name << std::endl;

                move_segment_to_device(table, table_name, chunk_id, column_id, device_name, new_segment_locations);

                moved_segment_count++;
            }
        }
        TieringSelectionPlugin::segment_locations = std::move(new_segment_locations);

        const auto run_end = std::chrono::steady_clock::now();

        std::cout << "Moved " << (int)moved_segment_count << " out of " << (int)config_segment_count << " segments to a different tier in " << std::chrono::duration_cast<std::chrono::duration<double>>(run_end - run_start).count() << " seconds." << std::endl;
        Assert(segment_count == static_cast<int64_t>(config_segment_count), "JSON did probably not include tiering specifications for all segments (of "
                                                                            "" + std::to_string(segment_count) +
                                                                                " segments, only "
                                                                                "" +
                                                                                std::to_string(config_segment_count) + " have been tiered)");
    }

    void handle_set_server_cores(const std::string command)
    {
        const auto last_space_pos = command.find_last_of(' ');
        const auto core_count = std::stoul(command.substr(last_space_pos + 1));

        std::stringstream ss;
        ss << "Set Hyrise scheduler to use " << core_count << " cores.";
        Hyrise::get().log_manager.add_message("TieringSelectionPlugin", ss.str(), LogLevel::Info);
        Hyrise::get().scheduler()->finish();
        Hyrise::get().topology.use_default_topology(core_count);
        // reset scheduler
        Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
    }

    void handle_apply_tiering_configuration(const std::string command)
    {
        std::cout << "Handle: apply_tiering_configuration" << std::endl;

        auto command_string = std::vector<std::string>{};
        boost::split(command_string, command, boost::is_any_of(" "), boost::token_compress_on);
        Assert(command_string.size() == 4,
               "Expecting one param. Usage: APPLY TIERING CONFIGURATION file");

        const auto file_path_str = command_string[3];
        if (command_string.size() == 4)
        {
            apply_tiering_configuration(file_path_str, std::thread::hardware_concurrency());
        }
    }

    void handle_run_calibration(const std::string command)
    {
        std::cout << "run calibration" << std::endl;

        auto command_strings = std::vector<std::string>{};
        boost::split(command_strings, command, boost::is_any_of(" "), boost::token_compress_on);
        Assert(command_strings.size() >= 4,
               "Expecting one param. Usage: RUN TIERING CALIBRATION <file> <device_1> <device_2> ...");

        const auto file_path_str = command_strings[3];
        auto devices = std::vector<std::string>(command_strings.begin() + 4, command_strings.end());
        tiering_calibration(file_path_str, devices);
    }

    void handle_set_devices(const std::string command)
    {
        std::cout << "set devices" << std::endl;

        auto command_strings = std::vector<std::string>{};
        boost::split(command_strings, command, boost::is_any_of(" "), boost::token_compress_on);
        Assert(command_strings.size() >= 2,
               "Expecting zero or more param. Usage: SET DEVICES <device_names>");

        MemoryResourceManager::devices = std::vector<std::string>(command_strings.begin() + 2, command_strings.end());
    }
} // namespace opossum

namespace opossum
{
    MetaTieringCommandTable::MetaTieringCommandTable()
        : AbstractMetaTable(TableColumnDefinitions{{"command", DataType::String, false}}) {}

    const std::string &MetaTieringCommandTable::name() const
    {
        static const auto name = std::string{"tiering_command"};
        return name;
    }

    std::shared_ptr<Table> MetaTieringCommandTable::_on_generate() const
    {
        auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

        const auto command = Hyrise::get().settings_manager.get_setting("Plugin::Tiering::Command")->get();
        if (command.starts_with("SET SERVER CORES "))
        {
            handle_set_server_cores(command);
        }
        else if (command.starts_with("APPLY TIERING CONFIGURATION "))
        {
            handle_apply_tiering_configuration(command);
        }
        else if (command.starts_with("RUN TIERING CALIBRATION"))
        {
            handle_run_calibration(command);
        }
        else if (command.starts_with("SET DEVICES "))
        {
            handle_set_devices(command);
        }
        else
        {
            output_table->append({pmr_string{"Unknown command."}});
            return output_table;
        }

        output_table->append({pmr_string{"Command executed successfully."}});
        return output_table;
    }
}

namespace opossum
{

    std::string TieringSelectionPlugin::description() const { return "This is the Hyrise TieringSelectionPlugin"; }

    void TieringSelectionPlugin::start()
    {
        std::cout << "starting tiering plugin" << std::endl;
        auto workload_command_executor_table = std::make_shared<MetaTieringCommandTable>();
        Hyrise::get().meta_table_manager.add_table(std::move(workload_command_executor_table));

        _command_setting = std::make_shared<TieringSetting>();
        _command_setting->register_at_settings_manager();
    }

    void TieringSelectionPlugin::stop()
    {
        _command_setting->unregister_at_settings_manager();
    }

    std::unordered_map<std::tuple<std::string, ChunkID, ColumnID>, std::string, segment_key_hash> TieringSelectionPlugin::segment_locations = {};

    EXPORT_PLUGIN(TieringSelectionPlugin)

} // namespace opossum
