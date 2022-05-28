/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.mn.cdc.data;


import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.SchemaBuilder;

/**
 * A semantic type for a Uuid string.
 * 
 * @author Horia Chiorean
 */
public class Uuid {

    public static final String LOGICAL_NAME = "cdc.data.Uuid";

    /**
     * Returns a {@link SchemaBuilder} for a Uuid field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     * 
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                            .name(LOGICAL_NAME)
                            .version(1);
    }

    /**
     * Returns a {@link SchemaBuilder} for a Uuid field, with all other default Schema settings.
     * 
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }
}
