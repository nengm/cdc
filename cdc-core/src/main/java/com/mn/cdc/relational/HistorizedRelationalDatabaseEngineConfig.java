/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.relational;


import com.mn.cdc.config.Configuration;
import com.mn.cdc.relational.history.HistoryRecordComparator;

/**
 * Configuration options shared across the relational CDC connectors which use a persistent database schema history.
 *
 * @author Gunnar Morling
 */
public abstract class HistorizedRelationalDatabaseEngineConfig extends RelationalDatabaseEngineConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    private boolean useCatalogBeforeSchema;
    private final String logicalName;
    private final Class<?> connectorClass;

    protected HistorizedRelationalDatabaseEngineConfig(Class<?> connectorClass, Configuration config, String logicalName,
                                                       Tables.TableFilter systemTablesFilter,
                                                       boolean useCatalogBeforeSchema, int defaultSnapshotFetchSize, ColumnFilterMode columnFilterMode) {
        super(config, logicalName, systemTablesFilter, TableId::toString, defaultSnapshotFetchSize, columnFilterMode);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.logicalName = logicalName;
        this.connectorClass = connectorClass;
    }

    protected HistorizedRelationalDatabaseEngineConfig(Class<?> connectorClass, Configuration config, String logicalName,
                                                       Tables.TableFilter systemTablesFilter, boolean useCatalogBeforeSchema, ColumnFilterMode columnFilterMode) {
        this(connectorClass, config, logicalName, systemTablesFilter, useCatalogBeforeSchema, DEFAULT_SNAPSHOT_FETCH_SIZE, columnFilterMode);
    }

    protected HistorizedRelationalDatabaseEngineConfig(Class<?> connectorClass, Configuration config, String logicalName,
                                                       Tables.TableFilter systemTablesFilter, Selectors.TableIdToStringMapper tableIdMapper,
                                                       boolean useCatalogBeforeSchema, ColumnFilterMode columnFilterMode) {
        super(config, logicalName, systemTablesFilter, tableIdMapper, DEFAULT_SNAPSHOT_FETCH_SIZE, columnFilterMode);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.logicalName = logicalName;
        this.connectorClass = connectorClass;
    }

    public boolean useCatalogBeforeSchema() {
        return useCatalogBeforeSchema;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure no history entries
     * newer than the offset we resume from are recovered (which could happen when restarting a connector after history
     * records have been persisted but no new offset has been committed yet).
     */
    protected abstract HistoryRecordComparator getHistoryRecordComparator();

}
