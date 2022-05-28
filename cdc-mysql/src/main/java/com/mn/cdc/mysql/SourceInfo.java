package com.mn.cdc.mysql;

import com.mn.cdc.common.BaseSourceInfo;
import com.mn.cdc.config.CommonEngineConfig;
import com.mn.cdc.document.Document;
import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.SchemaBuilder;
import com.mn.cdc.structure.Struct;
import com.mn.cdc.util.Collect;
import com.mn.cdc.util.GtidSet;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @program:cdc-master
 * @description 源信息，包括我们之前处理过的源二进制日志中的位置
 * @author:miaoneng
 * @create:2021-09-07 21:25
 **/
public class SourceInfo extends BaseSourceInfo {
    private String restartBinlogFilename;
    private long restartBinlogPosition = 0L;

    //binlog文件名
    private String currentBinlogFilename;

    //binlog当前位置
    private long currentBinlogPosition = 0L;

    //当前行数
    private int currentRowNumber = 0;

    //重启需要跳过的行数
    private int restartRowsToSkip = 0;

    //重启需要跳过的事件数
    private long restartEventsToSkip = 0;

    //服务ID
    private long serverId = 0;

    //重启Gtuid
    private String restartGtidSet;

    private long binlogTimestampSeconds = 0;

    //线程号
    private long threadId = -1L;

    private String serverName;

    private long currentEventLengthInBytes = 0;

    private String currentGtidSet;

    private String currentQuery = null;

    private Map<String, String> sourcePartition;

    private String currentGtid;

    private Set<TableId> tableIds;
    private String databaseName;

    private Instant sourceTime = null;
    private String databaseIncludeList;
    private String databaseExcludeList;
    private String tableIncludeList;
    private String tableExcludeList;

    public static final String SERVER_NAME_KEY = "name";
    public static final String SERVER_PARTITION_KEY = "server";
    public static final String GTID_SET_KEY = "gtids";
    public static final String GTID_KEY = "gtid";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String THREAD_KEY = "thread";
    public static final String DB_NAME_KEY = "db";
    public static final String TABLE_NAME_KEY = "table";
    public static final String QUERY_KEY = "query";
    public static final String SERVER_ID_KEY = "server_id";
    public static final String DATABASE_WHITELIST_KEY = "database_whitelist";
    public static final String DATABASE_INCLUDE_LIST_KEY = "database_include_list";
    public static final String DATABASE_BLACKLIST_KEY = "database_blacklist";
    public static final String DATABASE_EXCLUDE_LIST_KEY = "database_exclude_list";
    public static final String TABLE_WHITELIST_KEY = "table_whitelist";
    public static final String TABLE_INCLUDE_LIST_KEY = "table_include_list";
    public static final String TABLE_BLACKLIST_KEY = "table_blacklist";
    public static final String TABLE_EXCLUDE_LIST_KEY = "table_exclude_list";


    public static final String RESTART_PREFIX = "RESTART_";
    private boolean inTransaction = false;

    public SourceInfo(CommonEngineConfig connectorConfig) {
        super(connectorConfig);

        this.tableIds = new HashSet<>();
    }

    /*
     * @Description:设置server的逻辑标识
     * @Param: logicalId
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/21 9:37
     */
    public void setServerName(String logicalId) {
        this.serverName = logicalId;
        sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    /*
     * @Description:
     * @Param: 设置binlog读取位置
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/7 21:57
     */
    public void setBinlogStartPoint(String binlogFilename, long positionOfFirstEvent) {
        if (binlogFilename != null) {
            this.currentBinlogFilename = binlogFilename;
            this.restartBinlogFilename = binlogFilename;
        }
        assert positionOfFirstEvent >= 0;
        this.currentBinlogPosition = positionOfFirstEvent;
        this.restartBinlogPosition = positionOfFirstEvent;
        this.currentRowNumber = 0;
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
    }

    public Map<String, ?> offset() {
        return offsetUsingPosition(this.restartRowsToSkip);
    }

    private Map<String, Object> offsetUsingPosition(long rowsToSkip) {
        Map<String, Object> map = new HashMap<>();
        if (serverId != 0) map.put("server_id", serverId);
        if (restartGtidSet != null) {
            //将之前完成的GTID集与事件编号一起放入偏移量中
            map.put("gtids", restartGtidSet);
        }
        map.put("file", restartBinlogFilename);
        map.put("pos", restartBinlogPosition);
        if (restartEventsToSkip != 0) {
            map.put("event", restartEventsToSkip);
        }
        if (rowsToSkip != 0) {
            map.put("row", rowsToSkip);
        }
        if (binlogTimestampSeconds != 0) map.put("ts_sec", binlogTimestampSeconds);
        map.put("currentPos", currentBinlogPosition);
        return map;
    }

    /**
     * Get the name of the MySQL binary log file that has last been processed.
     *
     * @return the name of the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
     */
    public String binlogFilename() {
        return currentBinlogFilename;
    }

    /**
     * Get the position within the MySQL binary log file of the next event to be processed.
     *
     * @return the position within the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
     */
    public long binlogPosition() {
        return currentBinlogPosition;
    }

    protected static SchemaBuilder schemaBuilder() {
        return SchemaBuilder.struct()
                .field("version", Schema.OPTIONAL_STRING_SCHEMA)
                .field("connector", Schema.OPTIONAL_STRING_SCHEMA);
    }

    public static final Schema SCHEMA = schemaBuilder()
            .name("mysql.Source")
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .field(SERVER_ID_KEY, Schema.INT64_SCHEMA)
            .field(TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .field(GTID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BINLOG_FILENAME_OFFSET_KEY, Schema.STRING_SCHEMA)
            .field(BINLOG_POSITION_OFFSET_KEY, Schema.INT64_SCHEMA)
            .field(BINLOG_ROW_IN_EVENT_OFFSET_KEY, Schema.INT32_SCHEMA)
            .field(SNAPSHOT_KEY, SchemaBuilder.bool().optional().defaultValue(false).build())
            .field(THREAD_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .field(DB_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(QUERY_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    public void startNextTransaction() {
        // 识别到begin，重置到开始
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
        this.restartBinlogFilename = this.currentBinlogFilename;
        this.restartBinlogPosition = this.currentBinlogPosition;
        this.inTransaction = true;
    }

    public static boolean offsetsHaveFilterInfo(Map<String, ?> sourceOffset) {
        return sourceOffset != null &&
                sourceOffset.containsKey(DATABASE_BLACKLIST_KEY) ||
                sourceOffset.containsKey(DATABASE_WHITELIST_KEY) ||
                sourceOffset.containsKey(TABLE_BLACKLIST_KEY) ||
                sourceOffset.containsKey(TABLE_WHITELIST_KEY);
    }

    /*
     * @Description:设置生成事件的mysql线程标识
     * @Param: threadId
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/15 17:24
     */
    public void setBinlogThread(long threadId) {
        this.threadId = threadId;
    }

    /*
     * @Description:完成事务
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/26 11:17
     */
    public void commitTransaction() {
        this.restartGtidSet = this.currentGtidSet;
        this.restartBinlogFilename = this.currentBinlogFilename;
        this.restartBinlogPosition = this.currentBinlogPosition + this.currentEventLengthInBytes;
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
        this.inTransaction = false;
        this.currentQuery = null;
    }

    public Struct struct() {
        return struct(null);
    }

    public Struct struct(TableId tableId) {
        assert serverName != null;
        Struct result = new Struct(SCHEMA);
        result.put(SERVER_NAME_KEY, serverName);
        result.put(SERVER_ID_KEY, serverId);
        if (currentGtid != null) {
            // Don't put the GTID Set into the struct; only the current GTID is fine ...
            result.put(GTID_KEY, currentGtid);
        }
        result.put(BINLOG_FILENAME_OFFSET_KEY, currentBinlogFilename);
        result.put(BINLOG_POSITION_OFFSET_KEY, currentBinlogPosition);
        result.put(BINLOG_ROW_IN_EVENT_OFFSET_KEY, currentRowNumber);
        result.put(TIMESTAMP_KEY, binlogTimestampSeconds);
        if (threadId >= 0) {
            result.put(THREAD_KEY, threadId);
        }
        if (tableId != null) {
            result.put(DB_NAME_KEY, tableId.catalog());
            result.put(TABLE_NAME_KEY, tableId.table());
        }
        if (currentQuery != null) {
            result.put(QUERY_KEY, currentQuery);
        }
        return result;
    }
    /**
     * Get the string representation of the GTID range for the MySQL binary log file.
     *
     * @return the string representation of the binlog GTID ranges; may be null
     */
    public String gtidSet() {
        return this.currentGtidSet != null ? this.currentGtidSet : null;
    }

    /**
     * Record that a new GTID transaction has been started and has been included in the set of GTIDs known to the MySQL server.
     *
     * @param gtid    the string representation of a specific GTID that has been begun; may not be null
     * @param gtidSet the string representation of GTID set that includes the newly begun GTID; may not be null
     */
    public void startGtid(String gtid, String gtidSet) {
        this.currentGtid = gtid;
        if (gtidSet != null && !gtidSet.trim().isEmpty()) {
            // Remove all the newline chars that exist in the GTID set string ...
            String trimmedGtidSet = gtidSet.replaceAll("\n", "").replaceAll("\r", "");
            // Set the GTID set that we'll use if restarting BEFORE successful completion of the events in this GTID ...
            this.restartGtidSet = this.currentGtidSet != null ? this.currentGtidSet : trimmedGtidSet;
            // Record the GTID set that includes the current transaction ...
            this.currentGtidSet = trimmedGtidSet;
        }
    }

    public void startGtid(String gtid) {
        this.currentGtid = gtid;
    }

    /**
     * Get the Kafka Connect detail about the source "partition", which describes the portion of the source that we are
     * consuming. Since we're reading the binary log for a single database, the source partition specifies the
     * {@link #setServerName(String) database server}.
     * <p>
     * The resulting map is mutable for efficiency reasons (this information rarely changes), but should not be mutated.
     *
     * @return the source partition information; never null
     */
    public Map<String, String> partition() {
        return sourcePartition;
    }

    public void databaseEvent(String databaseName) {
        this.databaseName = databaseName;
    }

    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new HashSet<>(tableIds);
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }


    String getCurrentGtid() {
        return currentGtid;
    }

    String getCurrentBinlogFilename() {
        return currentBinlogFilename;
    }

    long getCurrentBinlogPosition() {
        return currentBinlogPosition;
    }

    int getCurrentRowNumber() {
        return currentRowNumber;
    }

    long getThreadId() {
        return threadId;
    }

    /**
     * Returns a string representation of the table(s) affected by the current
     * event. Will only represent more than a single table for events in the
     * user-facing schema history topic for certain types of DDL. Will be {@code null}
     * for DDL events not applying to tables (CREATE DATABASE etc.).
     */
    String table() {
        return tableIds.isEmpty() ? null
                : tableIds.stream()
                .filter(x -> x != null)
                .map(TableId::table)
                .collect(Collectors.joining(","));
    }

    /**
     * @return the original SQL query that generated the event.  NULL if no such query is associated.
     */
    public String getQuery() {
        return this.currentQuery;
    }

    long getServerId() {
        return serverId;
    }

    /*
     * @Description:设置时间戳，因为mysql的binlog事件的时间戳精确到秒，所以这边的精度只有秒，但是时间戳统一用毫秒表示
     * @Param: timestampInSeconds
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/20 17:11
     */
    public void setBinlogTimestampSeconds(long timestampInSeconds) {
        this.sourceTime = Instant.ofEpochSecond(timestampInSeconds);
    }

    public void setSourceTime(Instant timestamp) {
        sourceTime = timestamp;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        if (tableIds == null || tableIds.isEmpty()) {
            return databaseName;
        }
        final TableId tableId = tableIds.iterator().next();
        if (tableId == null) {
            return databaseName;
        }
        return tableId.catalog();
    }

    /**
     * Set the position in the MySQL binlog where we will start reading.
     *
     * @param binlogFilename       the name of the binary log file; may not be null
     * @param positionOfFirstEvent the position in the binary log file to begin processing
     */
    public void setBinlogPosition(String binlogFilename, long positionOfFirstEvent) {
        if (binlogFilename != null) {
            this.currentBinlogFilename = binlogFilename;
        }
        assert positionOfFirstEvent >= 0;
        this.currentBinlogPosition = positionOfFirstEvent;
        this.currentRowNumber = 0;
    }

    /*
     * @Description:设置mysql二进制文件中对应事件的位置
     * @Param: positionOfCurrentEvent 当前事件在二进制日志文件中的位置
     * @param: eventSizeInBytes 事件长度（字节）
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/21 20:16
     */
    public void setEventPosition(long positionOfCurrentEvent, long eventSizeInBytes) {
        this.currentBinlogPosition = positionOfCurrentEvent;
        this.currentEventLengthInBytes = eventSizeInBytes;
        if (!inTransaction) {
            this.restartBinlogPosition = positionOfCurrentEvent + eventSizeInBytes;
            this.restartRowsToSkip = 0;
            this.restartEventsToSkip = 0;
        }
        // Don't set anything else, since the row numbers are set in the offset(int,int) method called at least once
        // for each processed event
    }

    /**
     * Set the position within the MySQL binary log file of the <em>current event</em>.
     *
     * @param positionOfCurrentEvent the position within the binary log file of the current event
     */
    public void setEventPosition(long positionOfCurrentEvent) {
        this.currentBinlogPosition = positionOfCurrentEvent;
    }

    /*
     * @Description:发出事件
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/21 20:37
     */
    public void completeEvent() {
        ++restartEventsToSkip;
    }
    /**
     * Given the row number within a binlog event and the total number of rows in that event, compute the
     * Kafka Connect offset that is be included in the produced change event describing the row.
     * <p>
     * This method should always be called before {@link #struct()}.
     *
     * @param eventRowNumber the 0-based row number within the event for which the offset is to be produced
     * @see #struct()
     */
    public void setRowNumber(int eventRowNumber) {
        this.currentRowNumber = eventRowNumber;
    }

    /**
     * Set the original SQL query.
     *
     * @param query the original SQL query that generated the event.
     */
    public void setQuery(final String query) {
        this.currentQuery = query;
    }


    /*
     * @Description:设置在 MySQL二进制日志文件中找到的服务器ID
     * @Param: serverId 服务器ID
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/20 17:09
     */
    public void setBinlogServerId(long serverId) {
        this.serverId = serverId;
    }


    /*
     * @Description:设置位点
     * @Param: sourceOffset
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/18 16:05
     */
    public void setoffset(Map<String, ?> sourceOffset) {
        if (sourceOffset != null) {
            //设置GUIDs，可以为空
            setCompletedGtidSet((String) sourceOffset.get(GTID_SET_KEY));
            String binlogFilename = (String) sourceOffset.get(BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException("Source offset '" + BINLOG_FILENAME_OFFSET_KEY + "' 参数为空");
            }
            long binlogPosition = longOffsetValue(sourceOffset, BINLOG_POSITION_OFFSET_KEY);
            setBinlogPosition(binlogFilename, binlogPosition);
            //略过的row
            this.restartRowsToSkip = (int) longOffsetValue(sourceOffset, BINLOG_ROW_IN_EVENT_OFFSET_KEY);

            //略过的事件
            this.restartEventsToSkip = longOffsetValue(sourceOffset, EVENTS_TO_SKIP_OFFSET_KEY);
            if (sourceOffset.containsKey(DATABASE_INCLUDE_LIST_KEY)) {
                this.databaseIncludeList = (String) sourceOffset.get(DATABASE_INCLUDE_LIST_KEY);
            } else {
                this.databaseIncludeList = (String) sourceOffset.get(DATABASE_WHITELIST_KEY);
            }
            if (sourceOffset.containsKey(DATABASE_EXCLUDE_LIST_KEY)) {
                this.databaseExcludeList = (String) sourceOffset.get(DATABASE_EXCLUDE_LIST_KEY);
            } else {
                this.databaseExcludeList = (String) sourceOffset.get(DATABASE_BLACKLIST_KEY);
            }
            if (sourceOffset.containsKey(TABLE_INCLUDE_LIST_KEY)) {
                this.tableIncludeList = (String) sourceOffset.get(TABLE_INCLUDE_LIST_KEY);
            } else {
                this.tableIncludeList = (String) sourceOffset.get(TABLE_WHITELIST_KEY);
            }
            if (sourceOffset.containsKey(TABLE_EXCLUDE_LIST_KEY)) {
                this.tableExcludeList = (String) sourceOffset.get(TABLE_EXCLUDE_LIST_KEY);
            } else {
                this.tableExcludeList = (String) sourceOffset.get(TABLE_BLACKLIST_KEY);
            }
        }
    }

    /**
     * Set the GTID set that captures all of the GTID transactions that have been completely processed.
     *
     * @param gtidSet the string representation of the GTID set; may not be null, but may be an empty string if no GTIDs
     *                have been previously processed
     */
    public void setCompletedGtidSet(String gtidSet) {
        if (gtidSet != null && !gtidSet.trim().isEmpty()) {
            // Remove all the newline chars that exist in the GTID set string ...
            String trimmedGtidSet = gtidSet.replaceAll("\n", "").replaceAll("\r", "");
            this.currentGtidSet = trimmedGtidSet;
            this.restartGtidSet = trimmedGtidSet;
        }
    }


    private long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0L;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException("Source offset '" + key + "' 参数值为" + obj + "不能转为long");
        }
    }

    /**
     * Get the number of events after the last transaction BEGIN that we've already processed.
     *
     * @return the number of events in the transaction that have been processed completely
     */
    public long eventsToSkipUponRestart() {
        return restartEventsToSkip;
    }

    /**
     * Get the number of rows beyond the {@link #eventsToSkipUponRestart() last completely processed event} to be skipped
     * upon restart.
     *
     * @return the number of rows to be skipped
     */
    public int rowsToSkipUponRestart() {
        return restartRowsToSkip;
    }


    /**
     * Determine whether the first {@link #offset() offset} is at or before the point in time of the second
     * offset, where the offsets are given in JSON representation of the maps returned by {@link #offset()}.
     * <p>
     * This logic makes a significant assumption: once a MySQL server/cluster has GTIDs enabled, they will
     * never be disabled. This is the only way to compare a position with a GTID to a position without a GTID,
     * and we conclude that any position with a GTID is *after* the position without.
     * <p>
     * When both positions have GTIDs, then we compare the positions by using only the GTIDs. Of course, if the
     * GTIDs are the same, then we also look at whether they have snapshots enabled.
     *
     * @param recorded the position obtained from recorded history; never null
     * @param desired the desired position that we want to obtain, which should be after some recorded positions,
     *            at some recorded positions, and before other recorded positions; never null
     * @param gtidFilter the predicate function that will return {@code true} if a GTID source is to be included, or
     *            {@code false} if a GTID source is to be excluded; may be null if no filtering is to be done
     * @return {@code true} if the recorded position is at or before the desired position; or {@code false} otherwise
     */
    /*
     * @Description: 比较第一个参数位点是在第二位点之后还是在之前。
     * @Param: recorded
     * @param: desired
     * @param: gtidFilter
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/11/26 10:53
     */
    public static boolean isPositionAtOrBefore(Document recorded, Document desired, Predicate<String> gtidFilter) {
        String recordedGtidSetStr = recorded.getString(GTID_SET_KEY);
        String desiredGtidSetStr = desired.getString(GTID_SET_KEY);
        if (desiredGtidSetStr != null) {
            // The desired position uses GTIDs, so we ideally compare using GTIDs ...
            if (recordedGtidSetStr != null) {
                // Both have GTIDs, so base the comparison entirely on the GTID sets.
                GtidSet recordedGtidSet = new GtidSet(recordedGtidSetStr);
                GtidSet desiredGtidSet = new GtidSet(desiredGtidSetStr);
                if (gtidFilter != null) {
                    // Apply the GTID source filter before we do any comparisons ...
                    recordedGtidSet = recordedGtidSet.retainAll(gtidFilter);
                    desiredGtidSet = desiredGtidSet.retainAll(gtidFilter);
                }
                if (recordedGtidSet.equals(desiredGtidSet)) {
                    // They are exactly the same, which means the recorded position exactly matches the desired ...
                    if (!recorded.has(SNAPSHOT_KEY) && desired.has(SNAPSHOT_KEY)) {
                        // the desired is in snapshot mode, but the recorded is not. So the recorded is *after* the desired ...
                        return false;
                    }
                    // In all other cases (even when recorded is in snapshot mode), recorded is before or at desired GTID.
                    // Now we need to compare how many events in that transaction we've already completed ...
                    int recordedEventCount = recorded.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int desiredEventCount = desired.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int diff = recordedEventCount - desiredEventCount;
                    if (diff > 0) {
                        return false;
                    }

                    // Otherwise the recorded is definitely before or at the desired ...
                    return true;
                }
                // The GTIDs are not an exact match, so figure out if recorded is a subset of the desired ...
                return recordedGtidSet.isContainedWithin(desiredGtidSet);
            }
            // The desired position did use GTIDs while the recorded did not use GTIDs. So, we assume that the
            // recorded position is older since GTIDs are often enabled but rarely disabled. And if they are disabled,
            // it is likely that the desired position would not include GTIDs as we would be trying to read the binlog of a
            // server that no longer has GTIDs. And if they are enabled, disabled, and re-enabled, per
            // https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-failover.html all properly configured slaves that
            // use GTIDs should always have the complete set of GTIDs copied from the master, in which case
            // again we know that recorded not having GTIDs is before the desired position ...
            return true;
        }
        else if (recordedGtidSetStr != null) {
            // The recorded has a GTID but the desired does not, so per the previous paragraph we assume that previous
            // is not at or before ...
            return false;
        }

        // Both positions are missing GTIDs. Look at the servers ...
        int recordedServerId = recorded.getInteger(SERVER_ID_KEY, 0);
        int desiredServerId = recorded.getInteger(SERVER_ID_KEY, 0);
        if (recordedServerId != desiredServerId) {
            // These are from different servers, and their binlog coordinates are not related. So the only thing we can do
            // is compare timestamps, and we have to assume that the server timestamps can be compared ...
            long recordedTimestamp = recorded.getLong(TIMESTAMP_KEY, 0);
            long desiredTimestamp = recorded.getLong(TIMESTAMP_KEY, 0);
            return recordedTimestamp <= desiredTimestamp;
        }

        // First compare the MySQL binlog filenames that include the numeric suffix and therefore are lexicographically
        // comparable ...
        String recordedFilename = recorded.getString(BINLOG_FILENAME_OFFSET_KEY);
        String desiredFilename = desired.getString(BINLOG_FILENAME_OFFSET_KEY);
        assert recordedFilename != null;
        int diff = recordedFilename.compareToIgnoreCase(desiredFilename);
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The filenames are the same, so compare the positions ...
        int recordedPosition = recorded.getInteger(BINLOG_POSITION_OFFSET_KEY, -1);
        int desiredPosition = desired.getInteger(BINLOG_POSITION_OFFSET_KEY, -1);
        diff = recordedPosition - desiredPosition;
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The positions are the same, so compare the completed events in the transaction ...
        int recordedEventCount = recorded.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
        int desiredEventCount = desired.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
        diff = recordedEventCount - desiredEventCount;
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The completed events are the same, so compare the row number ...
        int recordedRow = recorded.getInteger(BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
        int desiredRow = desired.getInteger(BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
        diff = recordedRow - desiredRow;
        if (diff > 0) {
            return false;
        }

        // The binlog coordinates are the same ...
        return true;
    }

    /*
     * @Description: row的距离
     * @Param: eventRowNumber 事件的行号
     * @param: totalNumberOfRows 总共的行数
     * @return: java.util.Map<java.lang.String,java.lang.Object>
     * @Author: miaoneng
     * @Date: 2021/11/29 20:37
     */
    public Map<String, Object> offsetForRow(int eventRowNumber, int totalNumberOfRows) {

        this.currentRowNumber = eventRowNumber;
        if (eventRowNumber < (totalNumberOfRows - 1)) {

            //重启后前面的行都不需要了，因为此位点证明前面的都处理过了。
            this.restartRowsToSkip = this.currentRowNumber + 1;
            return offsetUsingPosition(this.restartRowsToSkip);
        }
        //已经是最后一行了，所以不需要略过前面的了
        this.restartRowsToSkip = 0;
        return offsetUsingPosition(totalNumberOfRows);
    }
}
