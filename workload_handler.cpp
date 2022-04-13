#include "workload_handler.hpp"

#include <fstream>

#include "abstract_benchmark_item_runner.hpp"
#include "benchmark_config.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "hyrise.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_constants.hpp"

namespace
{

    using namespace opossum; // NOLINT

    // Shamelessly copied from tpcds_benchmark.cpp. TODO(anyone): move to DS benchmark class?
    std::unordered_set<std::string> filename_blacklist()
    {
        auto filename_blacklist = std::unordered_set<std::string>{};
        const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
        std::ifstream blacklist_file(blacklist_file_path);

        if (!blacklist_file)
        {
            std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
        }
        else
        {
            std::string filename;
            while (std::getline(blacklist_file, filename))
            {
                if (filename.size() > 0 && filename.at(0) != '#')
                {
                    filename_blacklist.emplace(filename);
                }
            }
            blacklist_file.close();
        }
        return filename_blacklist;
    }

    class TPCHBenchmarkItemExporter : public TPCHBenchmarkItemRunner
    {
    public:
        TPCHBenchmarkItemExporter(const std::shared_ptr<BenchmarkConfig> &config)
            : TPCHBenchmarkItemRunner(config, false, 1.0f, ClusteringConfiguration::None) {}

        std::vector<std::string> get_sql_statements_for_benchmark_item(const BenchmarkItemID item_id)
        {
            auto unique_statements = std::unordered_set<std::string>{};

            constexpr auto MAX_TRIES = size_t{10'000};
            for (auto counter = size_t{0}; counter < MAX_TRIES; ++counter)
            {
                unique_statements.insert(_build_query(item_id));
            }

            const auto statements = std::vector<std::string>{unique_statements.begin(), unique_statements.end()};
            return statements;
        }
    };

    class FileBasedBenchmarkItemExporter : public FileBasedBenchmarkItemRunner
    {
    public:
        FileBasedBenchmarkItemExporter(const std::shared_ptr<BenchmarkConfig> &config, const std::string &query_path,
                                       const std::unordered_set<std::string> &filename_blacklist)
            : FileBasedBenchmarkItemRunner(config, query_path, filename_blacklist) {}

        std::vector<std::string> get_sql_statements_for_benchmark_item(const BenchmarkItemID item_id)
        {
            auto unique_statements = std::unordered_set<std::string>{};

            constexpr auto MAX_TRIES = size_t{10'000};
            for (auto counter = size_t{0}; counter < MAX_TRIES; ++counter)
            {
                // unique_statements.insert(_build_query(item_id));
            }

            const auto statements = std::vector<std::string>{unique_statements.begin(), unique_statements.end()};
            return statements;
        }

        std::vector<FileBasedBenchmarkItemRunner::Query> get_queries()
        {
            return _queries;
        }
    };

} // namespace

namespace opossum
{

    MetaBenchmarkItems::MetaBenchmarkItems()
        : AbstractMetaTable(TableColumnDefinitions{{"benchmark_name", DataType::String, false},
                                                   {"item_name", DataType::String, false},
                                                   {"sql_statement_string", DataType::String, false}}) {}

    const std::string &MetaBenchmarkItems::name() const
    {
        static const auto name = std::string{"benchmark_items"};
        return name;
    }

    std::shared_ptr<Table> MetaBenchmarkItems::_on_generate() const
    {
        auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

        auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());

        auto tpch_item_exporter = TPCHBenchmarkItemExporter{config};
        for (const auto item : tpch_item_exporter.items())
        {
            const auto item_instances = tpch_item_exporter.get_sql_statements_for_benchmark_item(item);
            for (const auto &item_instance : item_instances)
            {
                output_table->append({pmr_string{"TPC-H"}, pmr_string{tpch_item_exporter.item_name(item)}, pmr_string{item_instance}});
            }
        }

        // Query paths are currently copied. TODO(anybody): consider making them accessible.
        const auto file_based_benchmarks = std::vector<std::tuple<std::string, std::string, std::unordered_set<std::string>>>{
            {"Join Order Benchmark", "third_party/join-order-benchmark", std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"}},
            {"TPC-DS", "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification", filename_blacklist()}};

        for (const auto &[benchmark_name, query_path, filename_blacklist] : file_based_benchmarks)
        {
            auto file_based_item_exporter = FileBasedBenchmarkItemExporter(config, query_path, filename_blacklist);
            for (const auto &[name, sql] : file_based_item_exporter.get_queries())
            {
                output_table->append({pmr_string{benchmark_name}, pmr_string{name}, pmr_string{sql}});
            }
        }

        return output_table;
    }

    WorkloadHandlerPlugin::WorkloadHandlerPlugin() {}

    std::string WorkloadHandlerPlugin::description() const { return "This is the Hyrise WorkloadHandlerPlugin"; }

    void WorkloadHandlerPlugin::start()
    {
        auto workload_items_table = std::make_shared<MetaBenchmarkItems>();
        Hyrise::get().meta_table_manager.add_table(std::move(workload_items_table));
    }

    void WorkloadHandlerPlugin::stop() {}

    EXPORT_PLUGIN(WorkloadHandlerPlugin)

} // namespace opossum
