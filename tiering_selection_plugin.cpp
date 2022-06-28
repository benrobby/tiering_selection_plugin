#include "tiering_selection_plugin.hpp"
#include "tiering_calibration.hpp"

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
#include "visualization/pqp_visualizer.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "sql/sql_pipeline_builder.hpp"

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

    void visualize_query(const std::string &query, std::string conf_name, std::string query_id, std::string output_dir)
    {
        auto pipeline = SQLPipelineBuilder{query}.create_pipeline();
        const auto [pipeline_status, result_table] = pipeline.get_result_table();
        Assert(pipeline_status == SQLPipelineStatus::Success, "Unexpected pipeline status");

        const auto &lqps = pipeline.get_optimized_logical_plans();
        const auto &pqps = pipeline.get_physical_plans();

        GraphvizConfig graphviz_config;
        graphviz_config.format = "png";
        std::string path = output_dir + "/" + conf_name + "_" + query_id;
        PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, path + "_pqp.png");
        LQPVisualizer{graphviz_config, {}, {}, {}}.visualize(lqps, path + "_lqp.png");
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
        const auto num_commands = 10;
        Assert(command_strings.size() >= num_commands,
               "Expecting one param. Usage: RUN TIERING CALIBRATION <file> <scale_factor> <benchmark_min_time_seconds> <random_data_size_per_device_mb> <monotonic_access_stride> <num_concurrent_threads> <use_multithreaded_calibration> <device_1> <device_2> ...");

        const auto file_path_str = command_strings[3];
        const auto scale_factor = command_strings[4];
        const auto benchmark_min_time_seconds = command_strings[5];
        const auto random_data_size_per_device_mb = command_strings[6];
        const auto monotonic_access_stride = command_strings[7];
        const auto num_concurrent_threads = command_strings[8];
        const auto use_multithreaded_calibration = command_strings[9];

        auto devices = std::vector<std::string>(command_strings.begin() + num_commands, command_strings.end());
        tiering_calibration(file_path_str, devices, std::stod(scale_factor), std::stod(benchmark_min_time_seconds), std::stoi(random_data_size_per_device_mb), std::stoi(monotonic_access_stride), std::stoi(num_concurrent_threads), use_multithreaded_calibration == "True");
    }

    void handle_set_devices(const std::string command)
    {
        std::cout << "set devices" << std::endl;

        auto command_strings = std::vector<std::string>{};
        boost::split(command_strings, command, boost::is_any_of(" "), boost::token_compress_on);
        Assert(command_strings.size() >= 3,
               "Expecting zero or more param. Usage: SET DEVICES <umap_buf_size> <device_names>");

        MemoryResourceManager::umap_memory_resource_buf_size_bytes = std::stoi(command_strings[2]);
        MemoryResourceManager::devices = std::vector<std::string>(command_strings.begin() + 3, command_strings.end());
    }

    void handle_visualize_query(const std::string command)
    {
        std::cout << "visualize query" << std::endl;
        auto command_strings = std::vector<std::string>{};
        boost::split(command_strings, command, boost::is_any_of(";"), boost::token_compress_on);
        Assert(command_strings.size() == 5,
               "Expecting the following params. Usage: VIS_QUERY;<query_id>;<test_id>;<query_tmp_file_name>;<output_dir>");
        const auto query_id = command_strings[1];
        const auto test_id = command_strings[2];
        const auto query_tmp_file_name = command_strings[3];
        const auto output_dir = command_strings[4];

        std::ifstream query_stream(query_tmp_file_name);
        std::stringstream buffer;
        buffer << query_stream.rdbuf();
        const auto query_string = buffer.str();
        visualize_query(query_string, test_id, query_id, output_dir);
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
        else if (command.starts_with("VIS_QUERY;"))
        {
            handle_visualize_query(command);
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
