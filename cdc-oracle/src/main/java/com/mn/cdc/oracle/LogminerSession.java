package com.mn.cdc.oracle;

import com.mn.util.redis.RedisWhiteList;
import com.mn.cdc.oracle.conf.LogMinerConf;
import com.mn.cdc.oracle.entity.EventRow;
import com.mn.cdc.oracle.entity.EventRowData;
import com.mn.cdc.oracle.entity.OracleInfo;
import com.mn.cdc.oracle.entity.RecordLog;
import com.mn.cdc.oracle.listener.LogParser;
import com.mn.cdc.oracle.util.SqlUtil;
import com.mn.cdc.relational.Column;
import com.mn.cdc.relational.ColumnEditor;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.relational.Tables;
import com.mn.cdc.structure.BaseRecord;
import com.mn.cdc.util.ClassUtil;
import com.mn.cdc.util.ExceptionUtil;
import com.mn.cdc.util.RetryUtil;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @program:cdc-build-parent
 * @description
 * @author:miaoneng
 * @create:2022-03-07 09:36
 **/
public class LogminerSession {
    public static Logger LOG = LoggerFactory.getLogger(LogminerSession.class);
    /**
     * oracle数据源信息 *
     */
    public OracleInfo oracleInfo;
    /**
     * 加载到logminer里的日志文件中 最小的nextChange *
     */
    protected BigInteger startScn = null;
    /**
     * 加载到logminer里的日志文件中 最小的nextChange *
     */
    protected BigInteger endScn = null;

    //jdbc连接
    public Connection connection;

    //分界符
    public String SEPERATORWORD = "#!>-<!#";
    private PreparedStatement logMinerSelectStmt;
    //配置
    private final LogMinerConf logMinerConfig;

    //上一次监听表集合
    List<String> lastListenerList;

    private String lastListenerTables = "";
    public static final int RETRY_TIMES = 3;
    public static final int SLEEP_TIME = 2000;
    public static final String CDB_CONTAINER_ROOT = "CDB$ROOT";
    public static final String KEY_SEG_OWNER = "SEG_OWNER";
    public static final String KEY_TABLE_NAME = "TABLE_NAME";
    public static final String KEY_OPERATION = "OPERATION";
    public static final String KEY_OPERATION_CODE = "OPERATION_CODE";
    public static final String KEY_TIMESTAMP = "TIMESTAMP";
    public static final String KEY_SQL_REDO = "SQL_REDO";
    public static final String KEY_SQL_UNDO = "SQL_UNDO";
    public static final String KEY_CSF = "CSF";
    public static final String KEY_SCN = "SCN";
    public static final String KEY_CURRENT_SCN = "CURRENT_SCN";
    public static final String KEY_FIRST_CHANGE = "FIRST_CHANGE#";
    public static final String KEY_ROLLBACK = "ROLLBACK";
    public static final String KEY_ROW_ID = "ROW_ID";
    public static final String KEY_XID_USN = "XIDUSN";
    public static final String KEY_XID_SLT = "XIDSLT";
    public static final String KEY_XID_SQN = "XIDSQN";
    public static final String KEY_START_SCN = "START_SCN";
    public static final String KEY_COMMIT_SCN = "COMMIT_SCN";

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //起始位点
    public BigInteger foundationSCN = new BigInteger("-1");

    //存放开始cdc后所有读到的日志数据，会根据commit，rollback定期读走。
    public Map<String, List<RecordLog>> mapALLOracleData = new HashMap<String, List<RecordLog>>();
    /////////////实现CLOB--------
    Map<String, String> clobMap = new HashMap<String, String>();
    /////////////实现BLOB--------
    Map<String, String> blobMap = new HashMap<String, String>();

    //存放表的列
    Map<TableId, List<Column>> columnsByTable = new HashMap<>();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    public LogminerSession(LogMinerConf logMinerConfig) {
        this.logMinerConfig = logMinerConfig;

    }

    /*进行jdbc连接*/
    public void connect() {
        try {
            ClassUtil.forName(logMinerConfig.getDriverName(), getClass().getClassLoader());
            connection = RetryUtil.executeWithRetry(() -> DriverManager.getConnection(
                    logMinerConfig.getJdbcUrl(),
                    logMinerConfig.getUsername(),
                    logMinerConfig.getPassword()),
                    RETRY_TIMES,
                    SLEEP_TIME,
                    false
            );
            oracleInfo = getOracleInfo(connection);

            // 修改session级别的 NLS_DATE_FORMAT 值为 "YYYY-MM-DD HH24:MI:SS"，否则在解析日志时 redolog出现
            // TO_DATE('18-APR-21', 'DD-MON-RR')

            try (PreparedStatement preparedStatement =
                         connection.prepareStatement(SqlUtil.SQL_ALTER_NLS_SESSION_PARAMETERS)) {
                preparedStatement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
                preparedStatement.execute();
            }

            // cdb需要会话在CDB$ROOT里
            if (oracleInfo.isCdbMode()) {
                try (PreparedStatement preparedStatement =
                             connection.prepareStatement(
                                     String.format(
                                             SqlUtil.SQL_ALTER_SESSION_CONTAINER, CDB_CONTAINER_ROOT))) {
                    preparedStatement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
                    preparedStatement.execute();
                }
            }
            LOG.info(
                    "get connection successfully, url:{}, username:{}, Oracle info：{}",
                    logMinerConfig.getJdbcUrl(),
                    logMinerConfig.getUsername(),
                    oracleInfo);

        } catch (Exception e) {
            String message =
                    String.format(
                            "get connection failed，url:[%s], username:[%s], e:%s",
                            logMinerConfig.getJdbcUrl(),
                            logMinerConfig.getUsername(),
                            ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            // 出现异常 需要关闭connection,保证connection 和 session日期配置 生命周期一致
            closeResources(null, null, connection);
            throw new RuntimeException(message, e);
        }
    }

    /**
     * 关闭数据库连接资源
     */
    private void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        closeStmt(stmt);

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 获取oracle的信息
     */
    public static OracleInfo getOracleInfo(Connection connection) throws SQLException {
        OracleInfo oracleInfo = new OracleInfo();

        oracleInfo.setVersion(connection.getMetaData().getDatabaseMajorVersion());

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ENCODING)) {
            rs.next();
            oracleInfo.setEncoding(rs.getString(1));
        }

        // 目前只有19才会判断是否是cdb模式
        if (oracleInfo.getVersion() == 19) {
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(SqlUtil.SQL_IS_CDB)) {
                rs.next();
                oracleInfo.setCdbMode(rs.getString(1).equalsIgnoreCase("YES"));
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SqlUtil.SQL_IS_RAC)) {
            rs.next();
            oracleInfo.setRacMode(rs.getString(1).equalsIgnoreCase("TRUE"));
        }

        LOG.info("oracle info {}", oracleInfo);
        return oracleInfo;
    }

    /*进行cdc动作*/
    public List<BaseRecord> poll() {


        String listenerTables = RedisWhiteList.whilteListMap.get("CDC_OracleWhiteList").stream().collect(Collectors.joining(","));
        if (listenerTables.equals("")) {
            return null;
        }
        List<String> listenserList = RedisWhiteList.whilteListMap.get("CDC_OracleWhiteList").stream().collect(Collectors.toList());
        Set<TableId> setTableId = new HashSet<>();
        if (lastListenerList != listenserList) {
            //不一样就重新采集，这边可以优化，需要的表采集，不需要的去掉
            columnsByTable.clear();

            DatabaseMetaData metadata = null;
            try {
                metadata = connection.getMetaData();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

            for (String lT : listenserList) {
                String[] tables = lT.split("\\.");
                TableId tableId = new TableId(null, tables[0], tables[1]);
                setTableId.add(tableId);

                try (ResultSet columnMetadata = metadata.getColumns(null, null, tables[1], null)) {
                    while (columnMetadata.next()) {
                        String columnName = columnMetadata.getString(4);
                        System.out.println(columnName);
                        String type = columnMetadata.getString(6);
                        System.out.println(type);
                        // add all included columns
                        readTableColumn(columnMetadata, tableId, null).ifPresent(column -> {
                            columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                                    .add(column.create());
                        });
                    }
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }


        String logMinerSelectSql =
                SqlUtil.buildSelectSql(
                        listenerTables,
                        oracleInfo.isCdbMode());
        try {
            if (startScn == null) {
                startScn = getCurrentScn();
            }
            BigInteger endScn = getCurrentScn();
            //判断是否超过步长
            if (endScn.subtract(startScn)
                    .compareTo(BigInteger.valueOf((long) 10000)) == 1) {
                endScn = startScn.add(BigInteger.valueOf((long) 10000));
            }
            //测试////////////////////////////////////////////////////////////////////
//            startScn = new BigInteger("632265627");
//            endScn = new BigInteger("632265644");
            //////////////////////////////////////////////////////////////////////////
            //------------------------------------------------加载logminer的V$LOGMNR_CONTENTS---------------------------

            //startOrUpdateLogMiner(startScn, endScn);
            String startSql;
            try {
                checkAndResetConnection();
                startSql = SqlUtil.SQL_START_LOGMINER;
                CallableStatement logMinerStartStmt = connection.prepareCall(startSql);
                configStatement(logMinerStartStmt);

                if (logMinerConfig.getSupportAutoAddLog()) {
                    logMinerStartStmt.setString(1, startScn.toString());
                } else {
                    logMinerStartStmt.setString(1, startScn.toString());
                    logMinerStartStmt.setString(2, endScn.toString());
                }
                logMinerStartStmt.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


            //----------------------------------------------------------------------------------------------------------
            closeStmt();
            LOG.info(logMinerSelectSql);
            //进行睡眠，因为有些数据库产生的logminer日志有一定的滞后。这边进行个5秒多的睡眠
            Thread.sleep(5000);

            //--------------------------------------------------进行实际解析---------------------------------------------
            //检查连接
            checkAndResetConnection();
            PreparedStatement logMinerSelectStmt =
                    connection.prepareStatement(
                            logMinerSelectSql,
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            //设置上界和下界
            logMinerSelectStmt.setString(1, startScn.toString());
            logMinerSelectStmt.setString(2, endScn.toString());

            long before = System.currentTimeMillis();

            ResultSet logMinerData = logMinerSelectStmt.executeQuery();
            long timeConsuming = (System.currentTimeMillis() - before) / 1000;
            LOG.info(
                    "query LogMiner data, startScn:{},endScn:{},timeConsuming {}",
                    startScn,
                    endScn,
                    timeConsuming);


            //这一轮所有CommitSCN
            List<BigInteger> commitScnList = new ArrayList<>();
            //这一轮commit的数据，将这次commit涉及到的数据存到容器里
            List<BaseRecord> oraclePollDataList = new ArrayList<BaseRecord>();
            /**
             0 - INTERNAL
             1 - INSERT
             2 - DELETE
             3 - UPDATE
             5 - DDL
             6 - START
             7 - COMMIT
             9 - SELECT_LOB_LOCATOR
             10 - LOB_WRITE
             11 - LOB_TRIM
             25 - SELECT_FOR_UPDATE
             28 - LOB_ERASE
             34 - MISSING_SCN
             68 - XML DOC BEGIN
             70 - XML DOC WRITE
             71 - XML DOC END
             36 - ROLLBACK
             255 - UNSUPPORTED
             **/
            //content遍历
            while (logMinerData.next()) {
                String sqlLog;

                String sqlRedo = logMinerData.getString(KEY_SQL_REDO);
                BigInteger scn = new BigInteger(logMinerData.getString(KEY_SCN));
                String operation = logMinerData.getString(KEY_OPERATION);
                int operationCode = logMinerData.getInt(KEY_OPERATION_CODE);
                String tableName = logMinerData.getString(KEY_TABLE_NAME);
                String schema = logMinerData.getString(KEY_SEG_OWNER);
                Timestamp timestamp = logMinerData.getTimestamp(KEY_TIMESTAMP);
                String xidUsn = logMinerData.getString(KEY_XID_USN);
                String xidSLt = logMinerData.getString(KEY_XID_SLT);
                String xidSqn = logMinerData.getString(KEY_XID_SQN);

                String rowId = logMinerData.getString(KEY_ROW_ID);
                boolean rollback = logMinerData.getBoolean(KEY_ROLLBACK);

                if (operation == null) {
                    continue;
                }
                boolean hasMultiSql;
                // 用CSF来判断一条sql是在当前这一行结束，sql超过4000 字节，会处理成多行
                boolean isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
                // 是否存在多条SQL
                hasMultiSql = isSqlNotEnd;

                String lastString = "";
                while (isSqlNotEnd) {
                    logMinerData.next();
                    // redoLog 实际上不需要发生切割  但是sqlUndo发生了切割，导致redolog值为null，所以需要判断
                    lastString = logMinerData.getString(KEY_SQL_REDO);
                    if (Objects.nonNull(lastString)) {
                        sqlRedo += lastString;
                    }
                    lastString = "";
                    isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
                }

                //加入容器，用事务号作为键
                String featureKey = xidUsn + xidSLt + xidSqn;
                List<RecordLog> listRecordLog = mapALLOracleData.get(featureKey);
                if (listRecordLog == null) {
                    listRecordLog = new ArrayList<>();
                    mapALLOracleData.put(featureKey, listRecordLog);
                }


                //参考flinkx-cdc，有其他情况往这边加
                // oracle10中文编码且字符串大于4000，LogMiner可能出现中文乱码导致SQL解析异常
                if (sqlRedo != null) {
                    if (hasMultiSql && oracleInfo.isOracle10() && oracleInfo.isGbk()) {
                        String redo = sqlRedo.toString();
                        String hexStr = new String(Hex.encodeHex(redo.getBytes("GBK")));
                        boolean hasChange = false;
                        if (operationCode == 1 && hexStr.contains("3f2c")) {
                            LOG.info(
                                    "current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}",
                                    scn,
                                    redo,
                                    hexStr);
                            hasChange = true;
                            hexStr = hexStr.replace("3f2c", "272c");
                        }
                        if (operationCode != 1) {
                            if (hexStr.contains("3f20616e64")) {
                                LOG.info(
                                        "current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}",
                                        scn,
                                        redo,
                                        hexStr);
                                hasChange = true;
                                // update set "" = '' and "" = '' where "" = '' and "" = '' where后可能存在中文乱码
                                // delete from where "" = '' and "" = '' where后可能存在中文乱码
                                // ?空格and -> '空格and
                                hexStr = hexStr.replace("3f20616e64", "2720616e64");
                            }

                            if (hexStr.contains("3f207768657265")) {
                                LOG.info(
                                        "current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}",
                                        scn,
                                        redo,
                                        hexStr);
                                hasChange = true;
                                // ? where 改为 ' where
                                hexStr = hexStr.replace("3f207768657265", "27207768657265");
                            }
                        }

                        if (hasChange) {
                            sqlLog = new String(Hex.decodeHex(hexStr.toCharArray()), "GBK");
                            LOG.info("final redo sql is: {}", sqlLog);
                        } else {
                            sqlLog = sqlRedo.toString();
                        }
                    } else {
                        sqlLog = sqlRedo.toString();
                    }
                }

                //处理二进制
                if (operation.equalsIgnoreCase("LOB_WRITE")) {
                    StringBuffer lobString = new StringBuffer();
                    String sqlString = "";
                    //处理二进制
                    int update_b_i = sqlRedo.indexOf("into loc_b from");
                    int update_b_ii = sqlRedo.indexOf("where");
                    int update_b_iii = sqlRedo.indexOf(" for update;");

                    int update_c_i = sqlRedo.indexOf("into loc_c from");
                    int update_c_ii = sqlRedo.indexOf("where");
                    int update_c_iii = sqlRedo.indexOf(" for update;");

                    int loc_i = sqlRedo.indexOf("select \"");

                    int loc_c_ii = sqlRedo.indexOf("\" into loc_c from");

                    int loc_b_ii = sqlRedo.indexOf("\" into loc_b from");

                    int flagindex = 0;
                    if ((loc_i != -1) && (loc_c_ii != -1)) {
                        flagindex = 1;
                        String columnString = sqlRedo.substring(loc_i + 8, loc_c_ii);
                        String guid = xidUsn + xidSLt + xidSqn + SEPERATORWORD + scn + SEPERATORWORD + schema + SEPERATORWORD + tableName + SEPERATORWORD + columnString;
                        sqlString = "update " + sqlRedo.substring(update_c_i + 15, update_c_ii)
                                + "set \"" + columnString + "\"='EMPTY_CLOB()' "
                                + sqlRedo.substring(update_c_ii, update_c_iii);


                        if (sqlRedo.indexOf("buf_c := '") != -1) {
                            int clobi = sqlRedo.indexOf("buf_c := '");
                            int clobii = sqlRedo.indexOf("'; \n" + "  dbms_lob.write");
                            String nowclobString = sqlRedo.substring(clobi + 10, clobii);

                            lobString.append(nowclobString);

                            clobMap.put(guid, lobString.toString());
                        }

                    } else if ((loc_i != -1) && (loc_b_ii != -1)) {
                        flagindex = 2;
                        String columnString = sqlRedo.substring(loc_i + 8, loc_b_ii);
                        //二进制分多条信息，我们需要通过guid来合并这些信息，让信息完整
                        String guid = xidUsn + xidSLt + xidSqn + rowId + SEPERATORWORD + schema + SEPERATORWORD
                                + tableName + SEPERATORWORD + columnString;

                        sqlString = "update " + sqlRedo.substring(update_b_i + 15, update_b_ii)
                                + "set \"" + columnString + "\"='EMPTY_BLOB()' "
                                + sqlRedo.substring(update_b_ii, update_b_iii);

                        if (sqlRedo.indexOf("buf_b := HEXTORAW('") != -1) {
                            int blobi = sqlRedo.indexOf("buf_b := HEXTORAW('");
                            int blobii = sqlRedo.indexOf("'); \n" + "  dbms_lob.write");
                            String nowblobString = sqlRedo.substring(blobi + 19, blobii);

                            lobString.append(nowblobString);

                            blobMap.put(guid, lobString.toString());
                        }
                    }
                    //将xidUsn + xidSLt + xidSqn +"#"+sqlString.trim()作为特征字段
                    String featureKeyLob = xidUsn + xidSLt + xidSqn + "#" + sqlString.trim();
                    Iterator<RecordLog> iterRecordLog = listRecordLog.iterator();
                    RecordLog recordLog = null;
                    while (iterRecordLog.hasNext()) {
                        RecordLog rLog = iterRecordLog.next();
                        if (rLog.getFeatureKey().equalsIgnoreCase(featureKeyLob)) {
                            recordLog = rLog;
                            break;
                        }
                    }
                    if (recordLog == null) {
                        TableId t = new TableId(null,schema,tableName);
                        List<Column> columnList = columnsByTable.get(t);
                        recordLog = new RecordLog(
                                scn,
                                lobString.toString(),
                                xidUsn,
                                xidSLt,
                                xidSqn,
                                rowId,
                                operationCode,
                                tableName, featureKeyLob, null,columnList);
                        listRecordLog.add(recordLog);
                    } else {
                        recordLog.setSqlRedo(recordLog.getSqlRedo() + lobString.toString());
                    }
                }
                //到这边说明sql完整，已经找到
                else if (operation.equalsIgnoreCase("INSERT") || operation.equalsIgnoreCase("UPDATE") || operation.equalsIgnoreCase("DELETE")) {

                    net.sf.jsqlparser.statement.Statement stmt;
                    try {
                        stmt = CCJSqlParserUtil.parse(sqlRedo);
                    } catch (JSQLParserException e) {
                        LOG.info("sqlRedo = {}", sqlRedo);
                        stmt = CCJSqlParserUtil.parse(sqlRedo.replace("\\'", "\\ '"));
                    }
                    ArrayList<EventRowData> afterEventRowDataList = new ArrayList<>();
                    ArrayList<EventRowData> EventRowDataList = new ArrayList<>();

                    if (stmt instanceof Insert) {
                        LogParser.parseInsertStmt((Insert) stmt, EventRowDataList, afterEventRowDataList);
                    } else if (stmt instanceof Update) {
                        LogParser.parseUpdateStmt((Update) stmt, EventRowDataList, afterEventRowDataList, sqlRedo);
                    } else if (stmt instanceof Delete) {
                        LogParser.parseDeleteStmt((Delete) stmt, EventRowDataList, afterEventRowDataList);
                    }
                    EventRow eventRow = new EventRow(EventRowDataList, afterEventRowDataList);
                    TableId t = new TableId(null,schema,tableName);
                    List<Column> columnList = columnsByTable.get(t);
                    RecordLog recordLog = new RecordLog(
                            scn,
                            sqlRedo,
                            xidUsn,
                            xidSLt,
                            xidSqn,
                            rowId,
                            operationCode,
                            tableName, featureKey, eventRow,columnList);

                    listRecordLog.add(recordLog);
                }

                //commit
                //如果是commit，我们把识别到的redo_sql就可以推到下个流程中了，这样的做的好处就是这些都是真实已经发生在数据库中的
                //变化不用在额外处理rollback了
                else if (operationCode == 7) {
                    BigInteger start_scn = new BigInteger(logMinerData.getNString(KEY_START_SCN));
                    BigInteger commit_scn = new BigInteger(logMinerData.getNString(KEY_COMMIT_SCN));
                    if (start_scn.compareTo(new BigInteger("0")) != 0 && start_scn.compareTo(foundationSCN) <= 0) {
                        //此次说明这些commit数据需要再次从归档日志中拉起，然后塞入全局队列中
                        //////////////////  待实现  ////////////////////



                        ///////////////////////////////////////////////
                        //刷新
                        listRecordLog = mapALLOracleData.get(featureKey);
                    }

                    listRecordLog.stream().forEach(r ->
                            oraclePollDataList.add(r));
                    commitScnList.add(commit_scn);
                    //清理
                    mapALLOracleData.remove(featureKey);


                } else if (operationCode == 36) {
                    //ROLLBACK
                    //清理
                    mapALLOracleData.remove(featureKey);
                }
            }
            logMinerData.close();
            logMinerSelectStmt.close();
            commitScnList.sort(new Comparator<BigInteger>() {
                @Override
                public int compare(BigInteger o1, BigInteger o2) {
                    return o2.compareTo(o1);
                }
            });
            Iterator<Map.Entry<String, List<RecordLog>>> mapIterator = mapALLOracleData.entrySet().iterator();
            if (commitScnList.size() != 0) {
                while (mapIterator.hasNext()) {
                    Map.Entry<String, List<RecordLog>> entry = mapIterator.next();
                    List<RecordLog> rLs = entry.getValue();
                    Iterator<RecordLog> iter = rLs.iterator();
                    while (iter.hasNext()) {
                        RecordLog rl = iter.next();
                        if (rl.getScn().compareTo(commitScnList.get(0)) > 0) {
                            iter.remove();
                        }
                    }
                }

            }
            LOG.info(oraclePollDataList.toString());
            //关闭所有连接

            //包含二进制的数据，没有进行合并，后期可以合并，目前一条带有二进制的数据是拆的。因为二进制是要通过存储过程才能得到的。
            return oraclePollDataList;

        } catch (Exception e) {
            String message =
                    String.format(
                            "query logMiner data failed, sql:[%s], e: %s",
                            logMinerSelectSql, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }


    /**
     * 关闭logMinerSelectStmt
     */
    public void closeStmt() {
        try {
            if (logMinerSelectStmt != null && !logMinerSelectStmt.isClosed()) {
                logMinerSelectStmt.close();
            }
        } catch (SQLException e) {
            LOG.warn("Close logMinerSelectStmt error", e);
        }
        logMinerSelectStmt = null;
    }

    /**
     * 关闭Statement
     */
    private void closeStmt(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.warn("Close statement error", e);
        }
    }

    /*配置statement*/
    private void configStatement(java.sql.Statement statement) throws SQLException {
        if (logMinerConfig.getQueryTimeout() != null) {
            statement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
        }
    }

    //得到当前的scn
    public BigInteger getCurrentScn() {
        BigInteger currentScn = null;
        CallableStatement currentScnStmt = null;
        ResultSet currentScnResultSet = null;

        try {
            currentScnStmt = connection.prepareCall(SqlUtil.SQL_GET_CURRENT_SCN);
            configStatement(currentScnStmt);

            currentScnResultSet = currentScnStmt.executeQuery();
            while (currentScnResultSet.next()) {
                currentScn = new BigInteger(currentScnResultSet.getString(KEY_CURRENT_SCN));
            }

            return currentScn;
        } catch (SQLException e) {
            LOG.error("获取当前的SCN出错:", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(currentScnResultSet, currentScnStmt, null);
        }
    }

    public void checkAndResetConnection() {
        if (!isValid()) {
            connect();
        }
    }

    public boolean isValid() {
        try {
            return connection != null && connection.isValid(logMinerConfig.getQueryTimeout().intValue());
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Returns a {@link ColumnEditor} representing the current record of the given result set of column metadata, if
     * included in column.include.list.
     */
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        // Oracle drivers require this for LONG/LONGRAW to be fetched first.
        final String defaultValue = columnMetadata.getString(13);

        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }
            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            } catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            column.nativeType(resolveNativeType(column.typeName()));
            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));
            if (defaultValue != null) {
                getDefaultValue(column.create(), defaultValue).ifPresent(column::defaultValue);
            }
            return Optional.of(column);
        }

        return Optional.empty();
    }

    protected Optional<Object> getDefaultValue(Column column, String defaultValue) {
        // nothing to do by default; overwrite in database specific implementation
        return Optional.empty();
    }

    protected static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    /**
     * Provides a native type for the given type name.
     * <p>
     * There isn't a standard way to obtain this information via JDBC APIs so this method exists to allow
     * database specific information to be set in addition to the JDBC Type.
     *
     * @param typeName the name of the type whose native type we are looking for
     * @return A type constant for the specific database or -1.
     */
    protected int resolveNativeType(String typeName) {
        return Column.UNSET_INT_VALUE;
    }

    /**
     * Resolves the supplied metadata JDBC type to a final JDBC type.
     *
     * @param metadataJdbcType the JDBC type from the underlying driver's metadata lookup
     * @param nativeType       the database native type or -1 for unknown
     * @return the resolved JDBC type
     */
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        return metadataJdbcType;
    }
}
