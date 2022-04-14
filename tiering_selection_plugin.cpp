#include "tiering_selection_plugin.hpp"
#include "tiering_calibration.hpp"

#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include <boost/algorithm/string.hpp>

namespace opossum
{
    using namespace opossum; // NOLINT

    void apply_tiering_configuration(const std::string &json_configuration_path, size_t task_count = 0ul)
    {
        std::cerr << "apply tiering configuartion not implemented" << std::endl;
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

    std::shared_ptr<Table> handle_apply_tiering_configuration(const std::string command, std::shared_ptr<opossum::Table> output_table)
    {
        std::cout << "Handle: apply_tiering_configuration" << std::endl;

        std::stringstream ss;
        auto tiering_command_strings = std::vector<std::string>{};
        // Split benchmark name and sizing factor
        boost::split(tiering_command_strings, command, boost::is_any_of(" "), boost::token_compress_on);
        Assert(tiering_command_strings.size() == 4,
               "Expecting one param. Usage: APPLY TIERING CONFIGURATION file");

        const auto file_path_str = tiering_command_strings[3];
        if (tiering_command_strings.size() == 4)
        {
            apply_tiering_configuration(file_path_str, std::thread::hardware_concurrency());
            output_table->append({pmr_string{"Command executed successfully."}});
            return output_table;
        }

        return nullptr;
    }

    void handle_run_calibration(const std::string command)
    {
        std::cout << "run calibration" << std::endl;
        tiering_calibration();
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
            auto res = handle_apply_tiering_configuration(command, output_table);
            if (res != nullptr)
            {
                return res;
            }
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
