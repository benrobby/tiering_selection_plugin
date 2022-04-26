#pragma once

#include <unordered_map>
#include <vector>
#include <tuple>

#include "hyrise.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum
{
    using namespace opossum;

    struct segment_key_hash
    {
        size_t operator()(const std::tuple<std::string, opossum::ChunkID, opossum::ColumnID> &p) const
        {
            size_t hash{0};
            boost::hash_combine(hash, std::get<0>(p));
            boost::hash_combine(hash, std::get<1>(p));
            boost::hash_combine(hash, std::get<2>(p));

            return hash;
        }
    };

    class MetaTieringCommandTable : public AbstractMetaTable
    {
    public:
        MetaTieringCommandTable();
        const std::string &name() const final;

        void on_tables_loaded() {}

    protected:
        std::shared_ptr<Table> _on_generate() const final;
    };

    class TieringSelectionPlugin : public AbstractPlugin, public Singleton<TieringSelectionPlugin>
    {
    public:
        TieringSelectionPlugin() {}

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

    public:
        static std::unordered_map<std::tuple<std::string, ChunkID, ColumnID>, std::string, segment_key_hash> segment_locations;
    };

} // namespace opossum
