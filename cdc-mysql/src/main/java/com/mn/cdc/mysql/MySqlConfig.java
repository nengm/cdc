package com.mn.cdc.mysql;

import java.time.Duration;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-08 15:01
 **/
public class MySqlConfig {
    public static final Integer DEFAULT_MAX_QUEUE_SIZE = 8192;
    public static final Integer DEFAULT_MAX_BATCH_SIZE = 4096;
    public static final Long DEFAULT_POLL_INTERVAL_MILLIS = 500L;
    public static final Boolean KEEP_ALIVE = true;
    public static final Long KEEP_ALIVE_INTERVAL_MS = Duration.ofMinutes(1).toMillis();

}
