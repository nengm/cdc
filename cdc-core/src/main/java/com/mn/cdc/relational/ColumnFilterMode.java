/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.relational;

/**
 * Modes for column name filters, either including a catalog (database) or schema name.
 * 列名称过滤器的模式，包括catalog或schema模式
 * 列名过滤器
 * @author Gunnar Morling
 */
public enum ColumnFilterMode {

    CATALOG {
        @Override
        public TableId getTableIdForFilter(String catalog, String schema, String table) {
            return new TableId(catalog, null, table);
        }
    },
    SCHEMA {
        @Override
        public TableId getTableIdForFilter(String catalog, String schema, String table) {
            return new TableId(null, schema, table);
        }
    };

    public abstract TableId getTableIdForFilter(String catalog, String schema, String table);
}
