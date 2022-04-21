#pragma once
#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include "hyrise.hpp"
#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/encoding_type.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

#include <benchmark/benchmark.h>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>

namespace opossum
{
    using namespace opossum;                        // NOLINT
    using namespace opossum::expression_functional; // NOLINT

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

    void tiering_calibration(const std::string &file_path, std::vector<std::string> devices)
    {
        std::cout << "Tiering calibration from plugin" << std::endl;

        auto &sm = Hyrise::get().storage_manager;
        const auto scale_factor = 0.01f;
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
        auto lineitem_table = sm.get_table("lineitem");

        // Predicates as in TPC-H Q6, ordered by selectivity. Not necessarily the same order as determined by the optimizer
        std::shared_ptr<PQPColumnExpression> _tpchq6_discount_operand = pqp_column_(ColumnID{6}, lineitem_table->column_data_type(ColumnID{6}),
                                                                                    lineitem_table->column_is_nullable(ColumnID{6}), "");
        std::shared_ptr<BetweenExpression> _tpchq6_discount_predicate = std::make_shared<BetweenExpression>(
            PredicateCondition::BetweenInclusive, _tpchq6_discount_operand, value_(0.05), value_(0.70001));

        auto TieringCalibration = [&](benchmark::State &state, auto device_name)
        {
            // todo move table segments to device
            // todo access pattern
            for (auto _ : state)
            {
                // Todo(Ben) resolve data type, resolve segment type statt tablescan
                // poslisten erstellen mit reference_segment_test
                const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _tpchq6_discount_predicate);
                table_scan->execute();
            }
        };

        for (auto &device_name : devices)
        {
            std::cout << "Benchmarking device: " << device_name << std::endl;
            benchmark::RegisterBenchmark(("TieringCalibration sequential " + device_name).c_str(), TieringCalibration, device_name);
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
    }
}
