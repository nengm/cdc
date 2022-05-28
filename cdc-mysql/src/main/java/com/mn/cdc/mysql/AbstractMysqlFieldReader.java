/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.mysql;

import com.mn.cdc.relational.Column;
import com.mn.cdc.relational.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Abstract class for decode MySQL return value according to different protocols.
 *
 * @author yangjie
 */
public abstract class AbstractMysqlFieldReader implements MySqlFieldReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Object readField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        if (column.jdbcType() == Types.TIME) {
            return readTimeField(rs, columnIndex);
        }
        else if (column.jdbcType() == Types.DATE) {
            return readDateField(rs, columnIndex, column, table);
        }
        // This is for DATETIME columns (a logical date + time without time zone)
        // by reading them with a calendar based on the default time zone, we make sure that the value
        // is constructed correctly using the database's (or connection's) time zone
        else if (column.jdbcType() == Types.TIMESTAMP) {
            return readTimestampField(rs, columnIndex, column, table);
        }
        // JDBC's rs.GetObject() will return a Boolean for all TINYINT(1) columns.
        // TINYINT columns are reported as SMALLINT by JDBC driver
        else if (column.jdbcType() == Types.TINYINT || column.jdbcType() == Types.SMALLINT) {
            // It seems that rs.wasNull() returns false when default value is set and NULL is inserted
            // We thus need to use getObject() to identify if the value was provided and if yes then
            // read it again to get correct scale
            return rs.getObject(columnIndex) == null ? null : rs.getInt(columnIndex);
        }
        // DBZ-2673
        // It is necessary to check the type names as types like ENUM and SET are
        // also reported as JDBC type char
        else if ("CHAR".equals(column.typeName()) ||
                "VARCHAR".equals(column.typeName()) ||
                "TEXT".equals(column.typeName())) {
            return rs.getBytes(columnIndex);
        }
        else {
            return rs.getObject(columnIndex);
        }
    }

    protected abstract Object readTimeField(ResultSet rs, int columnIndex) throws SQLException;

    protected abstract Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    protected abstract Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;
}