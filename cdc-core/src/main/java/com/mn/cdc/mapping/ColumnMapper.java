/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.mapping;


import com.mn.cdc.config.Configuration;
import com.mn.cdc.relational.Column;
import com.mn.cdc.relational.ValueConverter;
import com.mn.cdc.structure.Field;
import com.mn.cdc.structure.SchemaBuilder;

/**
 * A factory for a function used to map values of a column.
 * 
 * @author Randall Hauch
 */
@FunctionalInterface
public interface ColumnMapper {
    
    /**
     * Initialize the ColumnMapper instance based upon the connector's configuration.
     * @param config the connector's configuration
     */
    default void initialize( Configuration config ) {
        // do nothing
    }

    /**
     * Create for the given column a function that maps values.
     * 
     * @param column the column description; never null
     * @return the function that converts the value; may be null
     */
    ValueConverter create(Column column);
    
    /**
     * Optionally annotate the schema with properties to better capture the mapping behavior.
     * @param column the column definition; never null
     * @param schemaBuilder the builder for the {@link Field}'s schema; never null
     */
    default void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
        // do nothing
    }
}
