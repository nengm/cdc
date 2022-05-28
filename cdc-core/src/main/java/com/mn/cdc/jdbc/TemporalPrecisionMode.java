/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.jdbc;


/**
 * The set of predefined TemporalPrecisionMode options.
 */
public enum TemporalPrecisionMode {
    /**
     * 根据数据库中的时间和日志使用当前time包下的语义来表示
     */
    ADAPTIVE("adaptive"),

    /**
     * 根据数据库中的时间和日志使用当前time包下的语义来表示。
     * TIME字段总是使用INT64表示微妙
     */
    ADAPTIVE_TIME_MICROSECONDS("adaptive_time_microseconds"),

    /**
     * 使用Kafka Connect表示时间和日志类型，具有毫秒精度，我们不用
     */
    CONNECT("connect");

    private final String value;

    TemporalPrecisionMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @return the matching option, or null if no match is found
     */
    public static TemporalPrecisionMode parse(String value) {
        if (value == null) return null;
        value = value.trim();
        for (TemporalPrecisionMode option : TemporalPrecisionMode.values()) {
            if (option.getValue().equalsIgnoreCase(value)) return option;
        }
        return null;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @param defaultValue the default value; may be null
     * @return the matching option, or null if no match is found and the non-null default is invalid
     */
    public static TemporalPrecisionMode parse(String value, String defaultValue) {
        TemporalPrecisionMode mode = parse(value);
        if (mode == null && defaultValue != null) mode = parse(defaultValue);
        return mode;
    }
}
