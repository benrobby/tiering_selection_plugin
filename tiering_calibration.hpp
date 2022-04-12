#pragma once
#include "storage/table.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include <boost/algorithm/string.hpp>

namespace opossum
{
    using namespace opossum; // NOLINT

    void tiering_calibration()
    {
        std::cout << "Tiering calibration from plugin" << std::endl;
        return;
        // const auto migration_start = std::chrono::steady_clock::now();
        // const auto segment_tiering_interval = 1;
        // if (segment_tiering_interval > 0)
        // {
        //     for (auto table : Hyrise::get().storage_manager.tables())
        //     {
        //         for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id)
        //         {
        //             for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id)
        //             {
        //                 if (chunk_id % n == 0)
        //                 {
        //                     move_segment_to_block_device(table, chunk_id, column_id);
        //                 }
        //             }
        //         }
        //     }
        // }
        // const auto migration_end = std::chrono::steady_clock::now();
        // const std::chrono::nanoseconds migration_duration = migration_end - migration_start;
        // std::cout << "- Segment migration time: " << migration_duration.count() << " ns.\n";
    }
}
