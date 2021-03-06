/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.mysql;

import com.mn.cdc.util.IoUtil;

import java.util.Properties;

/**
 * Information about this module.
 *
 * @author Randall Hauch
 */
public class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "mysql/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "mysql";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "MySQL";
    }
}
