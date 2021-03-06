/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.mysql;

import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * @author Randall Hauch
 */
public class StopEventDataDeserializer implements EventDataDeserializer<StopEventData> {

    @Override
    public StopEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        return new StopEventData();
    }
}
