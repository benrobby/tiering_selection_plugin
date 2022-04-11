#pragma once

#include "hyrise.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum
{

    class ExamplePlugin : public AbstractPlugin, public Singleton<ExamplePlugin>
    {
    public:
        ExamplePlugin() : sm{Hyrise::get().storage_manager} {}

        std::string description() const final;

        void start() final;

        void stop() final;

        StorageManager &sm;
    };

} // namespace opossum
