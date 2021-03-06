package com.mn.cdc.mysql;

import com.mn.cdc.config.Configuration;
import com.mn.cdc.relational.ColumnId;
import com.mn.cdc.relational.Selectors;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.util.Collect;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-13 11:24
 **/
public class Filters {
    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("mysql", "performance_schema","sys", "information_schema");

    protected static boolean isBuiltInDatabase(String databaseName) {
        if(databaseName == null) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase());
    }

    protected static boolean isBuiltInTable(TableId id ) {
        return isBuiltInDatabase(id.catalog());
    }

    protected static boolean isNotBuiltInDatabase(String databaseName) {
        return !isBuiltInDatabase(databaseName);
    }

    protected static boolean isNotBuiltInTable(TableId id ) {
        return !isBuiltInTable(id);
    }

    protected static List<TableId> withoutBuiltIns(Collection<TableId> tableIds) {
        return tableIds.stream().filter(Filters::isNotBuiltInTable).collect(Collectors.toList());
    }

    protected static List<String> withoutBuiltInDatabases(Collection<String> dbNames) {
        return dbNames.stream().filter(Filters::isNotBuiltInDatabase).collect(Collectors.toList());
    }

    private final Predicate<String> dbFilter;
    private final Predicate<TableId> tableFilter;
    private final Predicate<String> isBuiltInDb;
    private final Predicate<TableId> isBuiltInTable;
    private final Predicate<ColumnId> columnFilter;

    private Filters(Predicate<String> dbFilter,
                    Predicate<TableId> tableFilter,
                    Predicate<String> isBuiltInDb,
                    Predicate<TableId> isBuiltInTable,
                    Predicate<ColumnId> columnFilter) {
        this.dbFilter = dbFilter;
        this.tableFilter = tableFilter;
        this.isBuiltInDb = isBuiltInDb;
        this.isBuiltInTable = isBuiltInTable;
        this.columnFilter = columnFilter;
    }

    public Predicate<String> databaseFilter() {
        return dbFilter;
    }

    public Predicate<TableId> tableFilter() {
        return tableFilter;
    }

    public Predicate<TableId> builtInTableFilter() {
        return isBuiltInTable;
    }

    public Predicate<String> builtInDatabaseFilter() {
        return isBuiltInDb;
    }

    public Predicate<ColumnId> columnFilter() {
        return columnFilter;
    }

    public static class Builder {

        private Predicate<String> dbFilter;
        private Predicate<TableId> tableFilter;
        private Predicate<String> isBuiltInDb = Filters::isBuiltInDatabase;
        private Predicate<TableId> isBuiltInTable = Filters::isBuiltInTable;
        private Predicate<ColumnId> columnFilter;
        private final Configuration config;

        /**
         * Create a Builder for a filter.
         * Set the initial filter data to match the filter data in the given configuration.
         * @param config the configuration of the connector.
         */
        public Builder(Configuration config) {
            this.config = config;
            setFiltersFromStrings(config.getString("database.whitelist"),
                    config.getString("database.blacklist"),
                    config.getString("table.whitelist"),
                    config.getString("table.blacklist"));

            // Define the filter that excludes blacklisted columns, truncated columns, and masked columns ...
            this.columnFilter = Selectors.excludeColumns(config.getString("column.blacklist"));
        }

        /**
         * Completely reset the filter to match the filter info in the given offsets.
         * This will completely reset the filters to those passed in.
         * @param offsets The offsets to set the filter info to.
         * @return this
         */
        public Builder setFiltersFromOffsets(Map<String, ?> offsets) {
            setFiltersFromStrings((String)offsets.get(SourceInfo.DATABASE_WHITELIST_KEY),
                    (String)offsets.get(SourceInfo.DATABASE_BLACKLIST_KEY),
                    (String)offsets.get(SourceInfo.TABLE_WHITELIST_KEY),
                    (String)offsets.get(SourceInfo.TABLE_BLACKLIST_KEY));
            return this;
        }

        private void setFiltersFromStrings(String dbWhitelist,
                                           String dbBlacklist,
                                           String tableWhitelist,
                                           String tableBlacklist) {
            Predicate<String> dbFilter = Selectors.databaseSelector()
                    .includeDatabases(dbWhitelist)
                    .excludeDatabases(dbBlacklist)
                    .build();

            // Define the filter using the whitelists and blacklists for tables and database names ...
            Predicate<TableId> tableFilter = Selectors.tableSelector()
                    .includeDatabases(dbWhitelist)
                    .excludeDatabases(dbBlacklist)
                    .includeTables(tableWhitelist)
                    .excludeTables(tableBlacklist)
                    .build();

            // Ignore built-in databases and tables ...
            if (Boolean.valueOf(config.getString("table.ignore.builtin"))) {
                this.tableFilter = tableFilter.and(isBuiltInTable.negate());
                this.dbFilter = dbFilter.and(isBuiltInDb.negate());
            } else {
                this.tableFilter = tableFilter;
                this.dbFilter = dbFilter;
            }
        }

        /**
         * Set the filter to match the given other filter.
         * This will completely reset the filters to those passed in.
         * @param filters The other filter
         * @return this
         */
        public Builder setFiltersFromFilters(Filters filters) {
            this.dbFilter = filters.dbFilter;
            this.tableFilter = filters.tableFilter;
            this.isBuiltInDb = filters.isBuiltInDb;
            this.isBuiltInTable = filters.isBuiltInTable;
            this.columnFilter = filters.columnFilter;
            return this;
        }

        /**
         * Exclude all those tables included by the given filter.
         * @param otherFilter the filter
         * @return this
         */
        public Builder excludeAllTables(Filters otherFilter) {
            excludeDatabases(otherFilter.dbFilter);
            excludeTables(otherFilter.tableFilter);
            return this;
        }

        /**
         * Exclude all the databases that the given predicate tests as true for.
         * @param databases the databases to excluded
         * @return
         */
        public Builder excludeDatabases(Predicate<String> databases) {
            this.dbFilter = this.dbFilter.and(databases.negate());
            return this;
        }

        /**
         * Include all the databases that the given predicate tests as true for.
         * All databases previously included will still be included.
         * @param databases the databases to be included
         * @return
         */
        public Builder includeDatabases(Predicate<String> databases) {
            this.dbFilter = this.dbFilter.or(databases);
            return this;
        }

        /**
         * Exclude all the tables that the given predicate tests as true for.
         * @param tables the tables to be excluded.
         * @return this
         */
        public Builder excludeTables(Predicate<TableId> tables) {
            this.tableFilter = this.tableFilter.and(tables.negate());
            return this;
        }

        /**
         * Include the tables that the given predicate tests as true for.
         * Tables previously included will still be included.
         * @param tables the tables to be included.
         * @return this
         */
        public Builder includeTables(Predicate<TableId> tables) {
            this.tableFilter = this.tableFilter.or(tables);
            return this;
        }

        /**
         * Build the filters.
         * @return the {@link Filters}
         */
        public Filters build() {
            return new Filters(this.dbFilter,
                    this.tableFilter,
                    this.isBuiltInDb,
                    this.isBuiltInTable,
                    this.columnFilter);
        }
    }
}
