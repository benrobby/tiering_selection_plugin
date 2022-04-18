#include "tiering_selection_plugin.hpp"
#include "tiering_calibration.hpp"
#include <nlohmann/json.hpp>

#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include <boost/algorithm/string.hpp>
#include "sql/sql_pipeline_builder.hpp"
#include <fstream>

namespace opossum
{
    using namespace opossum; // NOLINT

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

        auto tiered_segment_count = std::atomic_int{0};
        auto moved_tiered_segment_count = std::atomic_int{0};
        auto &storage_manager = Hyrise::get().storage_manager;

        for (const auto &[table_name, segment_configs] : config["configuration"].items())
        {
            const auto &table = storage_manager.get_table(table_name);
            std::cout << "Migrating segments for table: " << table_name << std::endl;

            for (const auto &[i, segment_config] : segment_configs.items())
            {
                tiered_segment_count++;
                const auto chunk_id = segment_config["chunk_id"];
                const auto column_id = segment_config["column_id"];
                const auto device_name = segment_config["device_name"];

                // std::cout << "Moving segment with table_name: " << table_name << " chunk_id: " << chunk_id << " column_id: " << column_id << " to device_name: " << device_name << std::endl;
                // todo move segment to tier
                moved_tiered_segment_count++; // todo
            }
        }
        std::cout << "Moved " << (int)moved_tiered_segment_count << " out of " << (int)tiered_segment_count << " segments to a different tier." << std::endl;
        Assert(segment_count == static_cast<int64_t>(tiered_segment_count), "JSON did probably not include tiering specifications for all segments (of "
                                                                            "" + std::to_string(segment_count) +
                                                                                " segments, only "
                                                                                "" +
                                                                                std::to_string(tiered_segment_count) + " have been tiered)");
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
}

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

    EXPORT_PLUGIN(TieringSelectionPlugin)

} // namespace opossum
