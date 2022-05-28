package com.mn.cdc.relational;

import com.mn.cdc.function.Predicates;

import java.util.function.Predicate;

/**
 * @program:cdc-master
 * @description 定义谓词来确定是否使用表或者列。使用正则来匹配是否包含哪些表和列还是排除哪些表和列
 * @author:miaoneng
 * @create:2021-09-11 13:07
 **/
public class Selectors {
    /**
     * Obtain a new {@link TableSelectionPredicateBuilder builder} for a table selection predicate.
     *
     * @return the builder; never null
     */
    public static DatabaseSelectionPredicateBuilder databaseSelector() {
        return new DatabaseSelectionPredicateBuilder();
    }

    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * Implementations convert given {@link TableId}s to strings, so regular expressions can be applied to them for the
     * purpose of table filtering.
     */
    @FunctionalInterface
    public static interface TableIdToStringMapper {
        String toString(TableId tableId);
    }

    /**
     * A builder of a database predicate.
     */
    public static class DatabaseSelectionPredicateBuilder {
        private Predicate<String> dbInclusions;
        private Predicate<String> dbExclusions;

        /**
         * Specify the names of the databases that should be included. This method will override previously included and
         * {@link #excludeDatabases(String) excluded} databases.
         *
         * @param databaseNames the comma-separated list of database names to include; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public DatabaseSelectionPredicateBuilder includeDatabases(String databaseNames) {
            if (databaseNames == null || databaseNames.trim().isEmpty()) {
                dbInclusions = null;
            } else {
                dbInclusions = Predicates.includes(databaseNames);
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be excluded. This method will override previously {@link
         * #excludeDatabases(String) excluded} databases, although {@link #includeDatabases(String) including databases} overrides
         * exclusions.
         *
         * @param databaseNames the comma-separated list of database names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public DatabaseSelectionPredicateBuilder excludeDatabases(String databaseNames) {
            if (databaseNames == null || databaseNames.trim().isEmpty()) {
                dbExclusions = null;
            } else {
                dbExclusions = Predicates.excludes(databaseNames);
            }
            return this;
        }

        /**
         * Build the {@link Predicate} that determines whether a database identified by its name is to be included.
         *
         * @return the table selection predicate; never null
         * @see #includeDatabases(String)
         * @see #excludeDatabases(String)
         */
        public Predicate<String> build() {
            Predicate<String> dbFilter = dbInclusions != null ? dbInclusions : dbExclusions;
            return dbFilter != null ? dbFilter : (id) -> true;
        }
    }

    /**
     * Obtain a new {@link TableSelectionPredicateBuilder builder} for a table selection predicate.
     *
     * @return the builder; never null
     */
    public static TableSelectionPredicateBuilder tableSelector() {
        return new TableSelectionPredicateBuilder();
    }

    /**
     * A builder of a table predicate.
     */
    public static class TableSelectionPredicateBuilder {
        private Predicate<String> dbInclusions;
        private Predicate<String> dbExclusions;
        private Predicate<String> schemaInclusions;
        private Predicate<String> schemaExclusions;
        private Predicate<TableId> tableInclusions;
        private Predicate<TableId> tableExclusions;

        /**
         * Specify the names of the databases that should be included. This method will override previously included and
         * {@link #excludeDatabases(String) excluded} databases.
         *
         * @param databaseNames the comma-separated list of database names to include; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeDatabases(String databaseNames) {
            if (isEmpty(databaseNames)) {
                dbInclusions = null;
            } else {
                dbInclusions = Predicates.includes(databaseNames);
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be excluded. This method will override previously {@link
         * #excludeDatabases(String) excluded} databases, although {@link #includeDatabases(String) including databases} overrides
         * exclusions.
         *
         * @param databaseNames the comma-separated list of database names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeDatabases(String databaseNames) {
            if (isEmpty(databaseNames)) {
                dbExclusions = null;
            } else {
                dbExclusions = Predicates.excludes(databaseNames);
            }
            return this;
        }

        /**
         * Specify the names of the schemas that should be included. This method will override previously included and
         * {@link #excludeSchemas(String) excluded} schemas.
         *
         * @param schemaNames the comma-separated list of schema names to include; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeSchemas(String schemaNames) {
            if (isEmpty(schemaNames)) {
                schemaInclusions = null;
            } else {
                schemaInclusions = Predicates.includes(schemaNames);
            }
            return this;
        }

        /**
         * Specify the names of the schemas that should be excluded. This method will override previously {@link
         * #excludeSchemas(String) excluded} schemas, although {@link #includeSchemas(String)} including schemas} overrides
         * exclusions.
         *
         * @param schemaNames the comma-separated list of schema names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeSchemas(String schemaNames) {
            if (isEmpty(schemaNames)) {
                schemaExclusions = null;
            } else {
                schemaExclusions = Predicates.excludes(schemaNames);
            }
            return this;
        }

        /**
         * Specify the names of the tables that should be included. This method will override previously included and
         * {@link #excludeTables(String) excluded} table names.
         * <p>
         * Note that any specified tables that are in an {@link #excludeDatabases(String) excluded database} will not be included.
         *
         * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to include; may be null or
         *            empty
         * @param tableIdMapper an arbitrary converter used to convert TableId into String for pattern matching.
         *         Usually used to remove a component from tableId to simplify patterns.
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeTables(String fullyQualifiedTableNames, TableIdToStringMapper tableIdMapper) {
            if (isEmpty(fullyQualifiedTableNames)) {
                tableInclusions = null;
            }
            else {
                tableInclusions = Predicates.includes(fullyQualifiedTableNames, tableId -> tableIdMapper.toString(tableId));
            }

            return this;
        }

        /**
         * Specify the names of the tables that should be included. This method will override previously included and
         * {@link #excludeTables(String) excluded} table names.
         * <p>
         * Note that any specified tables that are in an {@link #excludeDatabases(String) excluded database} will not be included.
         *
         * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to include; may be null or
         *            empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeTables(String fullyQualifiedTableNames) {
            return includeTables(fullyQualifiedTableNames, TableId::toString);
        }

        /**
         * Specify the names of the tables that should be excluded. This method will override previously {@link
         * #excludeDatabases(String) excluded} tables, although {@link #includeTables(String) including tables} overrides
         * exclusions.
         * <p>
         * Note that any specified tables that are in an {@link #excludeDatabases(String) excluded database} will not be included.
         *
         * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to exclude; may be null or
         *            empty
         * @param tableIdMapper an arbitrary converter used to convert TableId into String for pattern matching.
         *         Usually used to remove a component from tableId to simplify patterns.
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeTables(String fullyQualifiedTableNames, TableIdToStringMapper tableIdMapper) {
            if (isEmpty(fullyQualifiedTableNames)) {
                tableExclusions = null;
            }
            else {
                tableExclusions = Predicates.excludes(fullyQualifiedTableNames, tableId -> tableIdMapper.toString(tableId));
            }

            return this;
        }

        /**
         * Specify the names of the tables that should be excluded. This method will override previously {@link
         * #excludeDatabases(String) excluded} tables, although {@link #includeTables(String) including tables} overrides
         * exclusions.
         * <p>
         * Note that any specified tables that are in an {@link #excludeDatabases(String) excluded database} will not be included.
         *
         * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to exclude; may be null or
         *            empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeTables(String fullyQualifiedTableNames) {
            return excludeTables(fullyQualifiedTableNames, TableId::toString);
        }

        /**
         * Build the {@link Predicate} that determines whether a table identified by a given {@link TableId} is to be included.
         *
         * @return the table selection predicate; never null
         * @see #includeDatabases(String)
         * @see #excludeDatabases(String)
         * @see #includeTables(String)
         * @see #excludeTables(String)
         * @see #includeSchemas(String)
         * @see #excludeSchemas(String)
         */
        public Predicate<TableId> build() {
            Predicate<TableId> tableFilter = tableInclusions != null ? tableInclusions : tableExclusions;
            Predicate<String> dbFilter = dbInclusions != null ? dbInclusions : dbExclusions;
            Predicate<String> schemaFilter = schemaInclusions != null ? schemaInclusions : schemaExclusions;

            if (dbFilter != null) {
                return buildStartingFromDbFilter(dbFilter, schemaFilter, tableFilter);
            }

            if (schemaFilter != null) {
                return buildStartingFromSchemaFilter(schemaFilter, tableFilter);
            }

            if (tableFilter != null) {
                return tableFilter;
            }

            return (id) -> true;
        }

        private Predicate<TableId> buildStartingFromSchemaFilter(Predicate<String> schemaFilter, Predicate<TableId> tableFilter) {
            assert schemaFilter != null;
            if (tableFilter != null) {
                return (id) -> schemaFilter.test(id.schema()) && tableFilter.test(id);
            } else {
                return (id) -> schemaFilter.test(id.schema());
            }
        }

        private Predicate<TableId> buildStartingFromDbFilter(Predicate<String> dbFilter, Predicate<String> schemaFilter,
                                                             Predicate<TableId> tableFilter) {
            assert dbFilter != null;

            if (schemaFilter != null) {
                if (tableFilter != null) {
                    return (id) -> dbFilter.test(id.catalog()) && schemaFilter.test(id.schema()) && tableFilter.test(id);
                } else {
                    return (id) -> schemaFilter.test(id.schema());
                }
            } else if (tableFilter != null) {
                return (id) -> dbFilter.test(id.catalog()) && tableFilter.test(id);
            } else {
                return (id) -> dbFilter.test(id.catalog());
            }
        }
    }

    /**
     * Build the {@link Predicate} that determines whether a column identified by a given {@link ColumnId} is to be included,
     * using the given comma-separated regular expression patterns defining which columns (if any) should be <i>excluded</i>.
     * <p>
     * Note that this predicate is completely independent of the table selection predicate, so it is expected that this predicate
     * be used only <i>after</i> the table selection predicate determined the table containing the column(s) is to be used.
     *
     * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names to exclude; may be null or
     * @return this builder so that methods can be chained together; never null
     */
    public static Predicate<ColumnId> excludeColumns(String fullyQualifiedColumnNames) {
        return Predicates.excludes(fullyQualifiedColumnNames, ColumnId::toString);
    }
}
