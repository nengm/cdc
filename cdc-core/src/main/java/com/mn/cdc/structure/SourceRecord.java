package com.mn.cdc.structure;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-08 14:06
 **/
public class SourceRecord implements BaseRecord{

    private final Schema keySchema;
    private final Object key;
    private final Schema valueSchema;
    private final Object value;
    private final Long timestamp;
    private final Map<String, ?> sourcePartition;
    private final Map<String, ?> sourceOffset;

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        Schema keySchema, Object key, Schema valueSchema, Object value) {
        this(sourcePartition, sourceOffset,keySchema, key, valueSchema, value, Timestamp.valueOf(LocalDateTime.now()).getTime());
    }

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                         Schema keySchema, Object key,
                         Schema valueSchema, Object value,
                         Long timestamp) {
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Object key() {
        return key;
    }

    public Schema keySchema() {
        return keySchema;
    }

    public Object value() {
        return value;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    public Long timestamp() {
        return timestamp;
    }
    public Map<String, ?> sourcePartition() {
        return sourcePartition;
    }

    public Map<String, ?> sourceOffset() {
        return sourceOffset;
    }

}
