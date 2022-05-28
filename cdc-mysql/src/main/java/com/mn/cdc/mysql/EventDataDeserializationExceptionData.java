package com.mn.cdc.mysql;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-09 10:04
 **/
public class EventDataDeserializationExceptionData implements EventData {
    private static final long serialVersionUID = 1L;

    private final EventDataDeserializationException cause;

    public EventDataDeserializationExceptionData(EventDataDeserializationException cause) {
        this.cause = cause;
    }

    public EventDataDeserializationException getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "EventDataDeserializationExceptionData [cause=" + cause + "]";
    }
}
