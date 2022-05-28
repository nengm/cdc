package com.mn.cdc.util;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-09 10:19
 **/
public class EnumUtil {
    public static enum EventProcessingFailureHandlingMode {
        /**
         * Problematic events will be skipped.
         */
        IGNORE("ignore"),

        /**
         * Problematic event and their binlog position will be logged and the events will be skipped.
         */
        WARN("warn"),

        /**
         * An exception indicating the problematic events and their binlog position is raised, causing the connector to be stopped.
         */
        FAIL("fail");

        private final String value;

        private EventProcessingFailureHandlingMode(String value) {
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
        public static EventProcessingFailureHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();

            for (EventProcessingFailureHandlingMode option : EventProcessingFailureHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) return option;
            }

            return null;
        }
    }
}
