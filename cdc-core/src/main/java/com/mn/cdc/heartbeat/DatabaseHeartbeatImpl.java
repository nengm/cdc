/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.heartbeat;

import com.mn.cdc.config.Field;
import com.mn.cdc.function.BlockingConsumer;
import com.mn.cdc.jdbc.JdbcConnection;
import com.mn.cdc.structure.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

/**
 *  Implementation of the heartbeat feature that allows for a DB query to be executed with every heartbeat.
 */
public class DatabaseHeartbeatImpl extends HeartbeatImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHeartbeatImpl.class);

    public static final String HEARTBEAT_ACTION_QUERY_PROPERTY_NAME = "heartbeat.action.query";

    public static final Field HEARTBEAT_ACTION_QUERY = Field.create(HEARTBEAT_ACTION_QUERY_PROPERTY_NAME)
            .withDisplayName("An optional query to execute with every heartbeat")
            .withType(Field.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED_HEARTBEAT, 2))
            .withWidth(Field.Width.MEDIUM)
            .withImportance(Field.Importance.LOW)
            .withDescription("The query executed with every heartbeat.");

    private final String heartBeatActionQuery;
    private final JdbcConnection jdbcConnection;
    private final HeartbeatErrorHandler errorHandler;

    DatabaseHeartbeatImpl(Duration heartbeatInterval, String topicName, String key, JdbcConnection jdbcConnection, String heartBeatActionQuery,
                          HeartbeatErrorHandler errorHandler) {
        super(heartbeatInterval, topicName, key);

        this.heartBeatActionQuery = heartBeatActionQuery;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        try {
            jdbcConnection.execute(heartBeatActionQuery);
        }
        catch (SQLException e) {
            if (errorHandler != null) {
                errorHandler.onError(e);
            }
            LOGGER.error("Could not execute heartbeat action (Error: " + e.getSQLState() + ")", e);
        }
        LOGGER.debug("Executed heartbeat action query");

        super.forcedBeat(partition, offset, consumer);
    }
}
