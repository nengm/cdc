/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.spi;

import com.mn.cdc.schema.DataCollectionId;
import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.Struct;

import java.time.Instant;
import java.util.Map;


/**
 * Keeps track of the current offset within the source DB's change stream. This reflects in the offset as committed to
 * Kafka and in the source info block contained within CDC messages themselves.
 *
 * @author Gunnar Morling
 *
 */
public interface OffsetContext {

    /**
     * Implementations load a connector-specific offset context based on the offset values stored in Kafka.
     */
    interface Loader<O extends OffsetContext> {
        O load(Map<String, ?> offset);
    }

    Map<String, ?> getOffset();

    Schema getSourceInfoSchema();

    Struct getSourceInfo();


    /**
     * Records the name of the collection and the timestamp of the last event
     */
    void event(DataCollectionId collectionId, Instant timestamp);

}
