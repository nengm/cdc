/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.basesourceinfo;

import com.mn.cdc.config.CommonEngineConfig;
import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.SchemaBuilder;
import com.mn.cdc.structure.Struct;
import com.mn.cdc.data.Enum;
import java.time.Instant;
import java.util.Objects;


/**
 * Common information provided by all connectors in either source field or offsets.
 * When this class schema changes the connector implementations should create
 * a legacy class that will keep the same behaviour.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfoStructMaker<T extends AbstractSourceInfo> implements SourceInfoStructMaker<T> {

    public static final Schema SNAPSHOT_RECORD_SCHEMA = Enum.builder("true,last,false").defaultValue("false").optional().build();

    private final String version;
    private final String connector;
    private final String serverName;

    public AbstractSourceInfoStructMaker(String connector, String version, CommonEngineConfig connectorConfig) {
        this.connector = Objects.requireNonNull(connector);
        this.version = Objects.requireNonNull(version);
        this.serverName = connectorConfig.getLogicalName();
    }

    protected SchemaBuilder commonSchemaBuilder() {
        return SchemaBuilder.struct()
                .field(AbstractSourceInfo.CDC_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.CDC_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected Struct commonStruct(T sourceInfo) {
        final Instant timestamp = sourceInfo.timestamp() == null ? Instant.now() : sourceInfo.timestamp();
        final String database = sourceInfo.database() == null ? "" : sourceInfo.database();
        Struct ret = new Struct(schema())
                .put(AbstractSourceInfo.CDC_VERSION_KEY, version)
                .put(AbstractSourceInfo.CDC_CONNECTOR_KEY, connector)
                .put(AbstractSourceInfo.SERVER_NAME_KEY, serverName)
                .put(AbstractSourceInfo.TIMESTAMP_KEY, timestamp.toEpochMilli())
                .put(AbstractSourceInfo.DATABASE_NAME_KEY, database);
        final String sequence = sourceInfo.sequence();
        if (sequence != null) {
            ret.put(AbstractSourceInfo.SEQUENCE_KEY, sequence);
        }
        return ret;
    }
}