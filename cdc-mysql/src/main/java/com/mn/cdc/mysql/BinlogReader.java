package com.mn.cdc.mysql;

import com.mn.util.redis.RedisWhiteList;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.mn.cdc.config.Configuration;
import com.mn.cdc.exception.CdcException;
import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.function.BlockingConsumer;
import com.mn.cdc.heartbeat.Heartbeat;
import com.mn.cdc.jdbc.JdbcConnection;
import com.mn.cdc.mysql.RecordMakers.RecordsForTable;
import com.mn.cdc.mysql.config.MySqlEngineConfig;
import com.mn.cdc.mysql.config.MySqlEngineConfig.SecureConnectionMode;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.util.*;
import com.mn.cdc.util.EnumUtil.EventProcessingFailureHandlingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-08 14:34
 **/
public class BinlogReader extends AbstractReader {
    private final static Logger logger = LoggerFactory.getLogger(BinlogReader.class);

    private static final long INITIAL_POLL_PERIOD_IN_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long MAX_POLL_PERIOD_IN_MILLIS = TimeUnit.HOURS.toMillis(1);
    private final SourceInfo sourceInfo;
    private final Clock clock;
    private final BinaryLogClient client;
    private final boolean recordSchemaChangesInSourceRecords = true;
    private final RecordMakers recordMakers;

    //心跳
    private Heartbeat heartbeat;
    private long initialEventsToSkip = 0L;

    private boolean skipEvent = false;

    private static final String KEEPALIVE_THREAD_NAME = "blc-keepalive";

    //不过滤二进制日志
    private boolean ignoreDmlEventByGtidSource = false;

    private final float heartbeatIntervalFactor = 0.7f;

    private final EnumUtil.EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode = EnumUtil.EventProcessingFailureHandlingMode.FAIL;

    private final EnumMap<EventType, BlockingConsumer<Event>> eventHandlers = new EnumMap<EventType, BlockingConsumer<Event>>(EventType.class);

    private final EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode = EventProcessingFailureHandlingMode.WARN;

    private int startingRowNumber = 0;

    private final Map<String, Thread> binaryLogClientThreads = new ConcurrentHashMap<>(4);

    //延迟时间控制接口
    private final ElapsedTimeStrategy pollOutputDelay;

    //mysql GtidSet
    private com.github.shyiko.mysql.binlog.GtidSet gtidSet;

    private volatile Map<String, ?> lastOffset = null;


    public static class BinlogPosition {
        final String filename;
        final long position;

        public BinlogPosition(String filename, long position) {
            assert filename != null;

            this.filename = filename;
            this.position = position;
        }

        public String getFilename() {
            return filename;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public String toString() {
            return filename + "/" + position;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + filename.hashCode();
            result = prime * result + (int) (position ^ (position >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BinlogPosition other = (BinlogPosition) obj;
            if (!filename.equals(other.filename)) {
                return false;
            }
            if (position != other.position) {
                return false;
            }
            return true;
        }
    }

    /*
     * @Description:
     * @Param: name binlogreader的名称
     * @param: mysqlTaskContext 任务上下文
     * @param: acceptAndContinue
     * @param: serverId
     * @return:
     * @Author: miaoneng
     * @Date: 2021/9/8 16:11
     */
    public BinlogReader(String name, MySqlTaskContext mysqlTaskContext, HaltingPredicate acceptAndContinue, long serverId) {
        super(name, mysqlTaskContext, acceptAndContinue);
        this.sourceInfo = mysqlTaskContext.getSource();
        this.clock = mysqlTaskContext.getClock();
        pollOutputDelay = ElapsedTimeStrategy.exponential(clock, INITIAL_POLL_PERIOD_IN_MILLIS, MAX_POLL_PERIOD_IN_MILLIS);

        recordMakers = mysqlTaskContext.makeRecord();
        System.out.println(mysqlTaskContext.hostname());
        System.out.println(mysqlTaskContext.port());
        System.out.println(mysqlTaskContext.username());
        System.out.println(mysqlTaskContext.password());

        this.client = new BinaryLogClient(mysqlTaskContext.hostname(), mysqlTaskContext.port(), mysqlTaskContext.username(), mysqlTaskContext.password());
        this.client.setThreadFactory(Threads.threadFactory(MySqlEngine.class, mysqlTaskContext.getEngineConfig().getLogicalName(), "binlog-client", false, false,
                x -> binaryLogClientThreads.put(x.getName(), x)));
        client.setServerId(serverId);
        client.setSSLMode(sslModeFor(mysqlJdbcContext.sslMode()));
        client.setKeepAlive(MySqlConfig.KEEP_ALIVE);

        //1分钟
        client.setKeepAliveInterval(MySqlConfig.KEEP_ALIVE_INTERVAL_MS);


        Configuration configuration = mysqlTaskContext.config();
        final long keepAliveInterval = configuration.getLong(MySqlEngineConfig.KEEP_ALIVE_INTERVAL_MS);
        client.setKeepAliveInterval(keepAliveInterval);

        //设置心跳为keepalive的0.4倍
        client.setHeartbeatInterval((long) (keepAliveInterval * heartbeatIntervalFactor));

        //注册binlog事件监听处理函数
        client.registerEventListener(this::handleEvent);

        client.registerLifecycleListener(new ReaderThreadLifecycleListener());

        // 使用附加类型设置事件解析器
        final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<Long, TableMapEventData>();
        EventDeserializer eventDeserializer = new EventDeserializer() {
            @Override
            public Event nextEvent(ByteArrayInputStream inputStream) throws IOException {
                try {
                    // Delegate to the superclass ...
                    Event event = super.nextEvent(inputStream);

                    // We have to record the most recent TableMapEventData for each table number for our custom deserializers ...
                    if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
                        TableMapEventData tableMapEvent = event.getData();
                        tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
                    }
                    return event;
                }
                // DBZ-217 In case an event couldn't be read we create a pseudo-event for the sake of logging
                catch (EventDataDeserializationException edde) {
                    // DBZ-3095 As of Java 15, when reaching EOF in the binlog stream, the polling loop in
                    // BinaryLogClient#listenForEventPackets() keeps returning values != -1 from peek();
                    // this causes the loop to never finish
                    // Propagating the exception (either EOF or socket closed) causes the loop to be aborted
                    // in this case
                    if (edde.getCause() instanceof IOException) {
                        throw edde;
                    }

                    EventHeaderV4 header = new EventHeaderV4();
                    header.setEventType(EventType.INCIDENT);
                    header.setTimestamp(edde.getEventHeader().getTimestamp());
                    header.setServerId(edde.getEventHeader().getServerId());

                    if (edde.getEventHeader() instanceof EventHeaderV4) {
                        header.setEventLength(((EventHeaderV4) edde.getEventHeader()).getEventLength());
                        header.setNextPosition(((EventHeaderV4) edde.getEventHeader()).getNextPosition());
                        header.setFlags(((EventHeaderV4) edde.getEventHeader()).getFlags());
                    }

                    EventData data = new EventDataDeserializationExceptionData(edde);
                    return new Event(header, data);
                }
            }
        };

        // Add our custom deserializers ...
        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new GtidEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(EventType.DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(
                        tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(
                        tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(
                        tableMapEventByTableId).setMayContainExtraInformation(true));
        client.setEventDeserializer(eventDeserializer);
    }

    /**
     * Create a binlog reader.
     *
     * @param name              the name of this reader; may not be null
     * @param context           the task context in which this reader is running; may not be null
     * @param acceptAndContinue see {@link AbstractReader# AbstractReader(String, MySqlTaskContext, Predicate)}
     */
    public BinlogReader(String name, MySqlTaskContext context, HaltingPredicate acceptAndContinue) {
        this(name, context, acceptAndContinue, context.serverId());
    }

    /*
     * @Description:注册所有binlog响应事件
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 14:06
     */
    @Override
    protected void doStart() {
        eventHandlers.put(EventType.STOP, this::handleServerStop);
        eventHandlers.put(EventType.HEARTBEAT, this::handleServerHeartbeat);
        //反序列化报错
        eventHandlers.put(EventType.INCIDENT, this::handleServerIncident);

        //日志正在切换，服务器已重新启动或者binlog已转换为新文件，在任何一种情况下，后续表号都将与目前看到的表号不同，所以要把缓存全部清理
        eventHandlers.put(EventType.ROTATE, this::handleRotateLogsEvent);
        eventHandlers.put(EventType.TABLE_MAP, this::handleUpdateTableMetadata);
        eventHandlers.put(EventType.QUERY, this::handleQueryEvent);

        //insert事件，基于行的复制模式mysql版本号为(5.1.16 - mysql-trunk
        eventHandlers.put(EventType.WRITE_ROWS, this::handleInsert);
        eventHandlers.put(EventType.UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.DELETE_ROWS, this::handleDelete);

        //insert事件，基于行的复制模式mysql版本号为5.1.18+
        eventHandlers.put(EventType.EXT_WRITE_ROWS, this::handleInsert);
        eventHandlers.put(EventType.EXT_UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.EXT_DELETE_ROWS, this::handleDelete);

        eventHandlers.put(EventType.VIEW_CHANGE, this::viewChange);

        //XA事务
        eventHandlers.put(EventType.XA_PREPARE, this::prepareTransaction);

        //事务的提交操作,普通事务是通过发送一个包含BEGIN语句和COMMIT语句，如果事务回滚则为ROLLBACK语句
        eventHandlers.put(EventType.XID, this::handleTransactionCompletion);

        final boolean isGtidModeEnabled = mysqlJdbcContext.isGtidModeEnabled();

        String availableServerGtidStr = mysqlJdbcContext.knownGtidSet();
        if (isGtidModeEnabled) {

            // GTIDs处理事件
            eventHandlers.put(EventType.GTID, this::handleGtidEvent);

            //当前server的GtidSet集
            GtidSet availableServerGtidSet = new GtidSet(availableServerGtidStr);

            //当前server清除了的GtidSet集
            GtidSet purgedServerGtidSet = mysqlJdbcContext.purgedGtidSet();

            //得到slave需要的GTID
            GtidSet filteredGtidSet = mysqlTaskContext.filterGtidSet(availableServerGtidSet, purgedServerGtidSet);

            if (filteredGtidSet != null) {
                logger.info("从GTID{}开始读取binlog日志", filteredGtidSet);

                //把1到**的全部排出在外
                String filteredGtidSetStr = filteredGtidSet.toString();
                client.setGtidSet(filteredGtidSetStr);
                sourceInfo.setCompletedGtidSet(filteredGtidSetStr);
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet(filteredGtidSetStr);

            } else {
                logger.info("不支持gtidset，只能通过记录Binlogfilename和BinlogPosition来定位日志了");
                client.setBinlogFilename(sourceInfo.binlogFilename());
                client.setBinlogPosition(sourceInfo.binlogPosition());
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet("");
            }

        } else {
            client.setBinlogFilename(sourceInfo.binlogFilename());
            client.setBinlogPosition(sourceInfo.binlogPosition());
        }

        //我们可能在事务中间重新启动，所以看看我们已经处理了多远的事务
        initialEventsToSkip = sourceInfo.eventsToSkipUponRestart();

        //设置起始行号，即下一个读取的行号
        startingRowNumber = sourceInfo.rowsToSkipUponRestart();

        //只有当我们到达第一个 BEGIN 事件时，我们才会开始跳过事件
        skipEvent = false;

        logger.info(Boolean.toString(isRunning()));
        if (isRunning()) {
            long timeout = mysqlTaskContext.getEngineConfig().getConnectionTimeout().toMillis();
            long started = mysqlTaskContext.getClock().currentTimeInMillis();
            try {
                client.connect(timeout);

                //需要等待keepalive线程运行，否则可能成为孤儿线程
                //这个问题和时间有关，如果连接过早调用关闭，keepalive线程不会终止，这样就会导致成为孤儿线程
                if (client.isKeepAlive()) {
                    logger.info("等待keepalive线程启动");
                    final Metronome metronome = Metronome.parker(Duration.ofMillis(100), clock);
                    int waitAttempts = 50;
                    boolean keepAliveThreadRunning = false;
                    while (!keepAliveThreadRunning && waitAttempts-- > 0) {
                        for (Thread t : binaryLogClientThreads.values()) {
                            if (t.getName().startsWith(KEEPALIVE_THREAD_NAME) && t.isAlive()) {
                                logger.info("Keepalive thread线程运行中");
                                keepAliveThreadRunning = true;
                            }
                        }
                        metronome.pause();
                    }

                }

            } catch (TimeoutException e) {
                //如果确实是超时了，直接报错
                long duration = clock.currentTimeInMillis() - started;
                if (duration > (0.9 * timeout)) {
                    double actualSeconds = TimeUnit.MILLISECONDS.toSeconds(duration);
                    throw new CdcException("Timed out after " + actualSeconds + " seconds while waiting to connect to MySQL at " +
                            mysqlTaskContext.getEngineConfig().hostname() + ":" + mysqlTaskContext.getEngineConfig().port() + " with user '" + mysqlTaskContext.getEngineConfig().username() + "'", e);
                }
                //如果没超时，是mysql服务器自己结束的，我们不管这个超时事件
            } catch (IOException e) {
                throw new CdcException("Failed to authenticate to the MySQL database at " +
                        mysqlTaskContext.getEngineConfig().hostname() + ":" + mysqlTaskContext.getEngineConfig().port() + " with user '" + mysqlTaskContext.getEngineConfig().username() + "'", e);
            } catch (Throwable e) {
                throw new CdcException("Unable to connect to the MySQL database at " +
                        mysqlTaskContext.getEngineConfig().hostname() + ":" + mysqlTaskContext.getEngineConfig().port() + " with user '" + mysqlTaskContext.getEngineConfig().username() + "': " + e.getMessage(), e);
            }
        }
    }

    /*
     * @Description:处理binlog事件
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/20 15:26
     */
    protected void handleEvent(Event event) {
        if (event == null) {
            return;
        }

        EventHeader eventHeader = event.getHeader();
        //logger.info(eventHeader.getEventType().toString());
        //心跳事件没有时间戳,如果事件不是心跳事件,则设置时间戳
        if (!eventHeader.getEventType().equals(EventType.HEARTBEAT)) {
            //秒转毫秒，binlog的时间戳只精确到秒
            sourceInfo.setBinlogTimestampSeconds(eventHeader.getTimestamp() / 1000L);
        }
        sourceInfo.setBinlogServerId(eventHeader.getServerId());
        EventType eventType = eventHeader.getEventType();


        //当MySql的binlog文件从file1滚动到file2的时候会发生此事件
        if (eventType == EventType.ROTATE) {
            EventData eventData = event.getData();
            RotateEventData rotateEventData;
            if (eventData instanceof EventDeserializer.EventDataWrapper) {
                rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
            } else {
                rotateEventData = (RotateEventData) eventData;
            }
            sourceInfo.setBinlogStartPoint(rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
        } else if (eventHeader instanceof EventHeaderV4) {
            EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
            sourceInfo.setEventPosition(trackableEventHeader.getPosition(), trackableEventHeader.getEventLength());
        }
        try {
            eventHandlers.getOrDefault(eventType, this::ignoreEvent).accept(event);

            //完成事件处理
            sourceInfo.completeEvent();
            if (skipEvent) {
                //我们处于跳过事件的模式，我们只是跳过了这个，所以减少我们的跳过计数
                --initialEventsToSkip;
                skipEvent = initialEventsToSkip > 0;
            }
        } catch (RuntimeException e) {
            //待完善
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            eventHandlers.clear();
            logger.info("因为线程中断，停止解析binlog事件");
        }
    }

    /*
     * @Description:处理GTID事件
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/22 15:32
     */
    public void handleGtidEvent(Event event) {
        logger.debug("GTID 事务：{}", event);
        GtidEventData gtidEventData = unwrapData(event);
        String gtid = gtidEventData.getGtid();
        gtidSet.add(gtid);
        //使用mysql GtidSet
        //记录一个新的GTID事务
        sourceInfo.startGtid(gtid, gtidSet.toString());

        //简化，不加入Gtid的filter事件
    }

    /*
     * @Description:处理mysql停止命令
     * @Param: event mysql事件
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 9:30
     */
    protected void handleServerStop(Event event) {
        logger.info("服务器停止：{}", event);
    }

    /*
     * @Description:HEARTBEAT从master发到slave，让master知道slaver还活着，这类事件不会出现在二进制日志或者中继日志中，而是由master通过转储事件线程直接发给slave，然后丢弃
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 9:33
     */
    protected void handleServerHeartbeat(Event event) {
        logger.info("Server heartbeat:{}", event);
    }

    /*
     * @Description:忽略事件
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/21 20:28
     */
    protected void ignoreEvent(Event event) {
        logger.trace("忽略事件:{}", event);
    }

    /*
     * @Description:反序列化报错
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 11:00
     */
    protected void handleServerIncident(Event event) {
        if (event.getData() instanceof EventDataDeserializationExceptionData) {
            EventDataDeserializationExceptionData data = event.getData();
            EventHeaderV4 eventHeaderV4 = (EventHeaderV4) data.getCause().getEventHeader();
            if (eventDeserializationFailureHandlingMode == EnumUtil.EventProcessingFailureHandlingMode.FAIL) {
                logger.error(
                        "反序列化binlog事件出错 offset {}.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        sourceInfo.offset(),
                        System.lineSeparator(),
                        eventHeaderV4.getPosition(),
                        eventHeaderV4.getNextPosition(),
                        sourceInfo.binlogFilename()
                );

                throw new RuntimeException(data.getCause());
            } else if (eventDeserializationFailureHandlingMode == EnumUtil.EventProcessingFailureHandlingMode.WARN) {
                logger.warn(
                        "反序列化binlog事件出错 offset {}.{}" +
                                "This exception will be ignored and the event be skipped.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        sourceInfo.offset(),
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeaderV4.getPosition(),
                        eventHeaderV4.getNextPosition(),
                        sourceInfo.binlogFilename(),
                        data.getCause()
                );
            } else {
                logger.error("Server incident: {}", event);
            }
        }
    }

    /*
     * @Description:日志正在切换，服务器已重新启动或者binlog已转换为新文件，在任何一种情况下，后续表号都将与目前看到的表号不同，所以要把缓存全部清理
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 11:14
     */
    protected void handleRotateLogsEvent(Event event) {
        logger.debug("Rotating logs:{}", event);
        RotateEventData command = unwrapData(event);
        assert command != null;
        recordMakers.clear();
    }

    /*
     * @Description:解包数据
     * @Param: event
     * @return: T
     * @Author: miaoneng
     * @Date: 2021/9/9 11:26
     */
    protected <T extends EventData> T unwrapData(Event event) {
        EventData eventData = event.getData();
        if (eventData instanceof EventDeserializer.EventDataWrapper) {
            eventData = ((EventDeserializer.EventDataWrapper) eventData).getInternal();
        }
        return (T) eventData;
    }

    /*
     * @Description:处理表元数据改变
     * 基于ROW格式的MySQL Binlog在记录DML语句的数据时，总会先写入一个table_map_event，这种类型的event用于记录表结构相关元数据信息，比如数据库名称，表名称，表的字段类型，表的字段元数据等等
     * 每个表映射事件都包括一个单调递增的数字标识符，后续都能使用，但是，如果表结构改变或者切换日志，就改变了！！！
     *
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 14:14
     */
    protected void handleUpdateTableMetadata(Event event) {
        TableMapEventData metadata = unwrapData(event);
        long tableNumber = metadata.getTableId();
        String databaseName = metadata.getDatabase();
        String tableName = metadata.getTable();
        if (!RedisWhiteList.whilteListMap.get("CDC_MysqlWhiteList").contains(databaseName + "." + tableName)) {
            return;
        }
        //databaseName与tableName确定tableId，不需要schemaName
        TableId tableId = new TableId(databaseName, null, tableName);
        if (recordMakers.assign(tableNumber, tableId)) {
            logger.debug("收到更新表元数据时间：｛｝", event);
        } else {
            //需要把数据结构导入，这边默认第一次接触到表没有更改过数据，或者说更改的记录已经在队列里面了，当你全部清除数据后，如果从中间自己选的位点开始消费，有可能出现途中经过几次数据
            //结构的调整，会产生问题的。所以第一次使用此插件后，历史数据结构会进行保存，就是为了规避这些问题，如果想清理历史表结构数据，此历史数据删除后建议就从当前位点开始消费。
            //如果里面有数据，但是不是从无到有记录到数据结构的，我们统一以当前数据库中的记录为初始化记录
            AtomicReference<String> sql = new AtomicReference<>();
            sql.set("SHOW CREATE TABLE " + "`" + tableId.catalog() + "`" + "." + "`" + tableId.table() + "`");
            SourceInfo sourceInfo = mysqlTaskContext.getSource();
            JdbcConnection jdbcConnection = mysqlJdbcContext.jdbc();
            try {
                jdbcConnection.query(sql.get(), rs -> {
                    if (rs.next()) {
                        mysqlTaskContext.mysqlSchema().applyDdl(sourceInfo, tableId.catalog(), rs.getString(2), (dbName, tables, statements) -> {
                            if (recordSchemaChangesInSourceRecords && recordMakers.schemaChanges(dbName, tables, statements, super::enqueueRecord) > 0) {
                                logger.debug("Recorded DDL statements for database '{}': {}", dbName, statements);
                            }
                        });
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
            informAboutUnknownTableIfRequired(event, tableId, "更新表元信息");
        }
    }

    /*
    Log_name  |Pos      |Event_type |Server_id|End_log_pos|Info                                                                                                                                                                                                                                                           |
    ----------|---------|-----------|---------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    bin.003576|327370161|Query      |       11|  327370257|BEGIN                                                                                                                                                                                                                                                          |
    bin.003576|327370257|Rows_query |       11|  327370524|# UPDATE QRTZ_TRIGGERS SET TRIGGER_STATE = 'ACQUIRED' WHERE SCHED_NAME = 'mn-customerservice-rest' AND TRIGGER_NAME = '框架统一消息定时发送服務每分钟异步执行1次' AND TRIGGER_GROUP = 'DEFAULT' AND TRIGGER_STATE = 'WAITING'
    bin.003576|327370524|Table_map  |       11|  327370637|table_id: 186140 (frame_customerservice_zwfw.qrtz_triggers)                                                                                                                                                                                                    |
    bin.003576|327370637|Update_rows|       11|  327372356|table_id: 186140 flags: STMT_END_F                                                                                                                                                                                                                             |
    bin.003576|327372356|Rows_query |       11|  327372829|# INSERT INTO QRTZ_FIRED_TRIGGERS (SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME, SCHED_TIME, STATE, JOB_NAME, JOB_GROUP, IS_NONCONCURRENT, REQUESTS_RECOVERY, PRIORITY) VALUES('mn-customerservice-rest', 'mn-customers|
    bin.003576|327372829|Table_map  |       11|  327372946|table_id: 186155 (frame_customerservice_zwfw.qrtz_fired_triggers)                                                                                                                                                                                              |
    bin.003576|327372946|Write_rows |       11|  327373189|table_id: 186155 flags: STMT_END_F                                                                                                                                                                                                                             |
    bin.003576|327373189|Xid        |       11|  327373220|COMMIT  xid=19142030                                                                                                                                                                                                                                       |
    bin.003576|327373220|Gtid       |       11|  327373285|SET @@SESSION.GTID_NEXT= '27b9014f-3404-11e8-bd6f-005056907adb:274358538'                                                                                                                                                                                      |
    bin.003576|327373285|Query      |       11|  327373376|BEGIN                                                                                                                                                                                                                                                          |
    bin.003576|327373376|Rows_query |       11|  327373628|# UPDATE QRTZ_TRIGGERS SET TRIGGER_STATE = 'ACQUIRED' WHERE SCHED_NAME = 'mn-customerservice-rest' AND TRIGGER_NAME = '框架soa同步服務每分钟异步执行1次' AND TRIGGER_GROUP = 'DEFAULT' AND TRIGGER_STATE = 'WAITING'
    bin.003576|327373628|Table_map  |       11|  327373736|table_id: 186107 (frame_customerservice.qrtz_triggers)                                                                                                                                                                                                         |
    bin.003576|327373736|Update_rows|       11|  327375385|table_id: 186107 flags: STMT_END_F                                                                                                                                                                                                                             |
    bin.003576|327375385|Rows_query |       11|  327375843|# INSERT INTO QRTZ_FIRED_TRIGGERS (SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME, SCHED_TIME, STATE, JOB_NAME, JOB_GROUP, IS_NONCONCURRENT, REQUESTS_RECOVERY, PRIORITY) VALUES('mn-customerservice-rest', 'mn-customers|
    bin.003576|327375843|Table_map  |       11|  327375955|table_id: 186110 (frame_customerservice.qrtz_fired_triggers)                                                                                                                                                                                                   |
    bin.003576|327375955|Write_rows |       11|  327376183|table_id: 186110 flags: STMT_END_F                                                                                                                                                                                                                             |
    bin.003576|327376183|Xid        |       11|  327376214|COMMIT  xid=19142041                                                                                                                                                                                                                                       |
    bin.003576|327376214|Gtid       |       11|  327376279|SET @@SESSION.GTID_NEXT= '27b9014f-3404-11e8-bd6f-005056907adb:274358539'                                                                                                                                                                                      |
    bin.003576|327376279|Query      |       11|  327376377|BEGIN                                                                                                                                                                                                                                                          |
    bin.003576|327376377|Rows_query |       11|  327376589|# INSERT INTO QRTZ_SCHEDULER_STATE (SCHED_NAME, INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL) VALUES('mn-customerservice-rest', '192-168-217-1391616064558441', 1631082672887, 7500)                                                                 |
    bin.003576|327376589|Table_map  |       11|  327376685|table_id: 186100 (frame_customerservice_3.1_test.qrtz_scheduler_state)                                                                                                                                                                                         |
    bin.003576|327376685|Write_rows |       11|  327376796|table_id: 186100 flags: STMT_END_F                                                                                                                                                                                                                             |
    bin.003576|327376796|Xid        |       11|  327376827|COMMIT  xid=19142115                                                                                                                                                                                                                                       |
    bin.003576|327376827|Gtid       |       11|  327376892|SET @@SESSION.GTID_NEXT= '27b9014f-3404-11e8-bd6f-005056907adb:274358540'
     */

    /*
     * @Description:执行更新语句时会生成此事件，包括：create，insert，update，delete；
     * QUERY_EVENT以文本的形式来记录事务的操作。
     * QUERY_EVENT类型的事件通常在以下几种情况下使用：1. 事务开始时，执行的BEGIN操作。2. STATEMENT格式中的DML操作3. ROW格式中的DDL操作
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/15 14:38
     */
    protected void handleQueryEvent(Event event) {
        QueryEventData queryEventData = unwrapData(event);
        String sql = queryEventData.getSql().trim();
        if (sql.equalsIgnoreCase("BEGIN")) {
            sourceInfo.startNextTransaction();
            sourceInfo.setBinlogThread(queryEventData.getThreadId());
            return;
        }
        if (sql.equalsIgnoreCase("COMMIT")) {
            handleTransactionCompletion(event);
            return;
        }

        String upperCasedStatementBegin = Strings.getBegin(sql, 7).toUpperCase();

        if (upperCasedStatementBegin.startsWith("XA ")) {
            // This is an XA transaction, and we currently ignore these and do nothing ...
            return;
        }
        if (mysqlTaskContext.ddlFilter().test(sql)) {
            logger.debug("DDL '{}' was filtered out of processing", sql);
            return;
        }
        if (upperCasedStatementBegin.equals("INSERT ") || upperCasedStatementBegin.equals("UPDATE ") || upperCasedStatementBegin.equals("DELETE ")) {
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                throw new RuntimeException(
                        "Received DML '" + sql + "' for processing, binlog probably contains events generated with statement or mixed based replication format");
            } else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn("Warning only: Received DML '" + sql
                        + "' for processing, binlog probably contains events generated with statement or mixed based replication format");
                return;
            } else {
                return;
            }
        }

        if (sql.equalsIgnoreCase("ROLLBACK")) {
            // ROLLBACK不支持
            logger.info("ROLLBACK事件暂时是不支持的，还能咋办");
        }
        if (!queryEventData.getDatabase().equals("")) {
            mysqlTaskContext.mysqlSchema()
                    .applyDdl(sourceInfo, queryEventData.getDatabase(), queryEventData.getSql(), (dbName, tables, statements) -> {
                        //看下tables是否在白名单列表里面，如果不在直接踢掉
                        for (TableId tableId : tables) {
                            if (!RedisWhiteList.whilteListMap.get("CDC_MysqlWhiteList").contains(tableId.id())) {
                                tables.remove(tableId);
                            }
                        }
                        //如果tables不为空，把信息发出去，数据有可能为建表信息
                        if (!tables.isEmpty()) {
                            if (recordSchemaChangesInSourceRecords && recordMakers.schemaChanges(dbName, tables, statements, super::enqueueRecord) > 0) {
                                logger.debug("Recorded DDL statements for database '{}': {}", dbName, statements);
                            }
                        }
                    });
        }

    }

    /*
     * @Description:事件发出commit
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/24 14:38
     */
    private void handleTransactionCompletion(Event event) {
        sourceInfo.commitTransaction();
        sourceInfo.setBinlogThread(-1L);
        skipEvent = false;
        ignoreDmlEventByGtidSource = false;
    }

    /*
     * @Description:包含了要插入的数据
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/24 14:39
     */
    protected void handleInsert(Event event) throws InterruptedException {
        if (skipEvent) {
            logger.debug("忽略以前处理过的事件：｛｝", event);
        }
        if (ignoreDmlEventByGtidSource) {
            logger.debug("GTID源头忽略dml事件: {}", event);
            return;
        }
        WriteRowsEventData writeRowsEventData = unwrapData(event);
        long tableNumber = writeRowsEventData.getTableId();
        BitSet includedColums = writeRowsEventData.getIncludedColumns();
        RecordsForTable recordsForTable = recordMakers.forTable(tableNumber, includedColums, super::enqueueRecord);
        if (recordsForTable != null) {
            List<Serializable[]> rows = writeRowsEventData.getRows();
            final Instant ts = mysqlTaskContext.getClock().currentTimeAsInstant();
            int count = 0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int row = startingRowNumber; row != numRows; row++) {
                    count += recordsForTable.create(rows.get(row), ts, row, numRows);
                }
                if (logger.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        logger.debug("记录一共{}条，插入最后{}条数据，事件为{}",
                                count, numRows - startingRowNumber, event);
                    } else {
                        logger.debug("事件{}插入{}条数据", count, event);
                    }
                }
            } else {
                // 之前处理过所有行
                logger.debug("跳过先前处理的插入事件: {}", event);
            }
        } else {
            //存在没识别到的row
            informAboutUnknownTableIfRequired(event, recordMakers.getTableIdFromTableNumber(tableNumber), "insert row");
        }
        startingRowNumber = 0;
    }

    /*
     * @Description:更新事件
     * @Param: event
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/29 11:28
     */
    protected void handleUpdate(Event event) throws InterruptedException {
        if (skipEvent) {
            logger.debug("忽略以前处理过的事件：｛｝", event);
        }
        if (ignoreDmlEventByGtidSource) {
            logger.debug("GTID源头忽略dml事件: {}", event);
            return;
        }
        UpdateRowsEventData update = unwrapData(event);
        long tableNumber = update.getTableId();
        BitSet includedColumns = update.getIncludedColumns();

        //拿到表的处理函数
        RecordsForTable recordsForTable = recordMakers.forTable(tableNumber, includedColumns, super::enqueueRecord);
        if (recordsForTable != null) {
            List<Map.Entry<Serializable[],Serializable[]>> rows = update.getRows();
            final Instant ts = mysqlTaskContext.getClock().currentTimeAsInstant();
            int count =0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int row = startingRowNumber; row != numRows; ++row) {
                    Map.Entry<Serializable[], Serializable[]> changes = rows.get(row);
                    Serializable[] before = changes.getKey();
                    Serializable[] after = changes.getValue();
                    count += recordsForTable.update(before, after, ts, row, numRows);
                }
                if (logger.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        logger.debug("Recorded {} update record(s) for last {} row(s) in event: {}",
                                count, numRows - startingRowNumber, event);
                    }
                    else {
                        logger.debug("Recorded {} update record(s) for event: {}", count, event);
                    }
                }
            }
            else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed update event: {}", event);
            }
        }
        startingRowNumber = 0;
    }
    protected void handleDelete(Event event) throws InterruptedException {
        if(skipEvent){
            logger.info("跳过已经处理过的事情:{}",event);
            return;
        }
        if(ignoreDmlEventByGtidSource){
            logger.info("跳过DML事件，因为此GTID已被过滤:{}",event);
            return;
        }
        DeleteRowsEventData deleted = unwrapData(event);
        long tableNumber = deleted.getTableId();
        BitSet incluedColumns = deleted.getIncludedColumns();
        RecordsForTable recordsForTable = recordMakers.forTable(tableNumber,incluedColumns,super::enqueueRecord);
        if(recordsForTable != null){
            List<Serializable[]> rows = deleted.getRows();
            final Instant ts = mysqlTaskContext.getClock().currentTimeAsInstant();
            int count = 0;
            int numRows = rows.size();
            if(startingRowNumber<numRows){
                for(int row = startingRowNumber;row != numRows;++row){
                    count += recordsForTable.delete(rows.get(row),ts,row,numRows);
                }
            }else{
                logger.debug("跳过之前处理的删除事件: {}", event);
            }
        }
        startingRowNumber = 0;
    }

    protected void viewChange(Event event) throws InterruptedException {
        logger.debug("View Change event: {}", event);
        // do nothing

    }
    protected void prepareTransaction(Event event) throws InterruptedException {
        logger.debug("XA Prepare event: {}", event);
        // do nothing
    }

    @Override
    protected void doStop() {
        try {
            if (client.isConnected()) {
                logger.debug("Stopping binlog reader '{}', last recorded offset: {}", this.name(), lastOffset);
                client.disconnect();
            }
            cleanupResources();
        } catch (IOException e) {
            logger.error("Unexpected error when disconnecting from the MySQL binary log reader '{}'", this.name(), e);
        }
    }

    @Override
    protected void doCleanup() {
        logger.debug("Completed writing all records that were read from the binlog before being stopped");
    }

    /*
     * @Description: 如果我们收到一个受监控但其元数据解析出来不是我们知道的事件，我们根据EventProcessingFailureHandlingMode的级别确定如何写入日志
     * @Param: event
     * @param: tableId
     * @param: typeToLog
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/29 10:48
     */
    private void informAboutUnknownTableIfRequired(Event event, TableId tableId, String typeToLog) {
        if (tableId != null && mysqlTaskContext.mysqlSchema().isTableMonitored(tableId)) {
            EventHeaderV4 eventHeader = event.getHeader();

            if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                logger.error(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        sourceInfo.offset(),
                        tableId,
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        sourceInfo.binlogFilename()
                );
                throw new ConnectException("Encountered change event for table " + tableId + "whose schema isn't known to this connector");
            } else if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}" +
                                "The event will be ignored.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        sourceInfo.offset(),
                        tableId,
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        sourceInfo.binlogFilename()
                );
            } else {
                logger.debug(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}" +
                                "The event will be ignored.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        sourceInfo.offset(),
                        tableId,
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        sourceInfo.binlogFilename()
                );
            }
        } else {
            logger.debug("Skipping {} event: {} for non-monitored table {}", typeToLog, event, tableId);
        }
    }

    protected final class ReaderThreadLifecycleListener implements BinaryLogClient.LifecycleListener {
        @Override
        public void onDisconnect(BinaryLogClient client) {
            System.out.println(client);
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            System.out.println(client);
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
            logger.info("A communication failure event arrived", ex);
            logReaderState();
            try {
                // Stop BinaryLogClient background threads
                client.disconnect();
            } catch (final Exception e) {
                logger.debug("Exception while closing client", e);
            }
            BinlogReader.this.failed(ex);
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                logger.debug("A deserialization failure event arrived", ex);
                logReaderState();
                BinlogReader.this.failed(ex);
            } else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn("A deserialization failure event arrived", ex);
                logReaderState(Level.WARN);
            } else {
                logger.debug("A deserialization failure event arrived", ex);
                logReaderState(Level.DEBUG);
            }
        }
    }

    private void logReaderState() {
        logReaderState(Level.ERROR);
    }

    private void logReaderState(Level severity) {
        final Object position = client == null ? "N/A" : client.getBinlogFilename() + "/" + client.getBinlogPosition();
        final String message = "Error during binlog processing. Last offset stored = {}, binlog reader near position = {}";
        switch (severity) {
            case WARN:
                logger.warn(message, lastOffset, position);
                break;
            case DEBUG:
                logger.debug(message, lastOffset, position);
                break;
            default:
                logger.error(message, lastOffset, position);
        }
    }

    protected static SSLMode sslModeFor(SecureConnectionMode mode) {
        switch (mode) {
            case DISABLED:
                return SSLMode.DISABLED;
            case PREFERRED:
                return SSLMode.PREFERRED;
            case REQUIRED:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_IDENTITY:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }


}
