/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.mapping;

import com.mn.cdc.relational.Column;
import com.mn.cdc.relational.ValueConverter;
import com.mn.cdc.structure.SchemaBuilder;

import java.util.Locale;

/**
 * A column mapper that adds the {@link #TYPE_NAME_PARAMETER_KEY} and {@link #TYPE_LENGTH_PARAMETER_KEY} schema
 * parameter keys.
 *
 * @author Orr Ganani
 * @author Gunnar Morling
 */
public class PropagateSourceTypeToSchemaParameter implements ColumnMapper {

    private static final String TYPE_NAME_PARAMETER_KEY = "__cdc.source.column.type";
    private static final String TYPE_LENGTH_PARAMETER_KEY = "__cdc.source.column.length";
    private static final String TYPE_SCALE_PARAMETER_KEY = "__cdc.source.column.scale";

    @Override
    public ValueConverter create(Column column) {
       return null;
    }

    @Override
    public void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
        // upper-casing type names to be consistent across connectors
       schemaBuilder.parameter(TYPE_NAME_PARAMETER_KEY, column.typeName().toUpperCase(Locale.ENGLISH));

       if (column.length() != Column.UNSET_INT_VALUE) {
           schemaBuilder.parameter(TYPE_LENGTH_PARAMETER_KEY, String.valueOf(column.length()));
       }

       if (column.scale().isPresent()) {
         schemaBuilder.parameter(TYPE_SCALE_PARAMETER_KEY, String.valueOf(column.scale().get()));
       }
    }
}
