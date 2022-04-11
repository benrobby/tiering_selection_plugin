#pragma once

#include "hyrise.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum
{

    class MetaTieringCommandTable : public AbstractMetaTable
    {
    public:
        MetaTieringCommandTable();
        const std::string &name() const final;

        void on_tables_loaded() {}

    protected:
        std::shared_ptr<Table> _on_generate() const final;
    };

    class TieringPlugin : public AbstractPlugin, public Singleton<TieringPlugin>
    {
    public:
        TieringPlugin() {}

        std::string description() const final;

        void start() final;

        void stop() final;

    private:
        class TieringSetting : public AbstractSetting
        {
        public:
            TieringSetting() : AbstractSetting("Plugin::Tiering::Command") {}
            const std::string &description() const final
            {
                static const auto description = std::string{"Command. Syntax does not matter as long as it is handled in the plugin."};
                return description;
            }
            const std::string &get() final { return _value; }
            void set(const std::string &value) final { _value = value; }

            std::string _value = "No op";
        };

        std::shared_ptr<TieringSetting> _command_setting;
    };

} // namespace opossum
