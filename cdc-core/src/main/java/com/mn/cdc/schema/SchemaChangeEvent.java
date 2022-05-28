/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.schema;


import com.mn.cdc.relational.Table;
import com.mn.cdc.relational.history.TableChanges;
import com.mn.cdc.structure.Struct;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * 数据库模式的结构更改
 *
 * @author Gunnar Morling
 */
public class SchemaChangeEvent {

    private final String database;
    private final String schema;
    private final String ddl;
    private final Set<Table> tables;
    private final SchemaChangeEventType type;
    private final Struct source;
    private TableChanges tableChanges = new TableChanges();

    public SchemaChangeEvent(Struct source, String database, String schema, String ddl, Table table,
                             SchemaChangeEventType type) {
        this(source, database, schema, ddl, table != null ? Collections.singleton(table) : Collections.emptySet(), type);
    }

    public SchemaChangeEvent( Struct source, String database, String schema, String ddl, Set<Table> tables,
                             SchemaChangeEventType type) {
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
        // schema is not mandatory for all databases
        this.schema = schema;
        // DDL is not mandatory
        this.ddl = ddl;
        this.tables = Objects.requireNonNull(tables, "tables must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        switch (type) {
            case CREATE:
                tables.forEach(tableChanges::create);
                break;
            case ALTER:
                tables.forEach(tableChanges::alter);
                break;
            case DROP:
                tables.forEach(tableChanges::drop);
                break;
            case DATABASE:
                break;
        }
    }
    public Struct getSource() {
        return source;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getDdl() {
        return ddl;
    }

    public Set<Table> getTables() {
        return tables;
    }

    public SchemaChangeEventType getType() {
        return type;
    }

    public TableChanges getTableChanges() {
        return tableChanges;
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent [database=" + database + ", schema=" + schema + ", ddl=" + ddl + ", tables=" + tables
                + ", type=" + type + "]";
    }

    /**
     * Type describing the content of the event.
     * CREATE, ALTER, DROP - corresponds to table operations
     * DATABASE - an event common to the database, like CREATE/DROP DATABASE or SET...
     */
    public static enum SchemaChangeEventType {
        CREATE,
        ALTER,
        DROP,
        DATABASE;
    }
}
