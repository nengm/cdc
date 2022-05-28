package com.mn.cdc.mysql;

import com.mn.cdc.config.Configuration;
import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.iengine.IEngine;
import com.mn.cdc.jdbc.JdbcConnection;
import com.mn.cdc.structure.BaseRecord;
import com.mn.cdc.structure.SourceRecord;
import com.mn.cdc.util.Clock;
import com.mn.cdc.util.ElapsedTimeStrategy;
import com.mn.cdc.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-07 16:08
 **/
public class MySqlEngine implements IEngine {

    private final Logger logger = LoggerFactory.getLogger(MySqlEngine.class);


    private volatile MySqlTaskContext mysqlTaskContext;
    private volatile MySqlJdbcContext mysqlJdbcContext;


    //用于控制start(),stop(),commitRecord()顺序调用
    private final ReentrantLock stateLock = new ReentrantLock();
    private volatile ElapsedTimeStrategy restartDelay;
    private final AtomicReference<Reader.State> state = new AtomicReference<Reader.State>(Reader.State.STOPPED);

    private volatile Configuration config;

    Map<String, ?> offsets;
    //读日志控制器
    private volatile ChainedReader readers;

    private Duration retriableRestartWait;

    /*
     * @Description:mysql引擎
     * @Param: properties 引擎带入属性
     * @param: offsets  位点
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/30 10:30
     */
    @Override
    public synchronized void start(Configuration config, Map<String, ?> offsets) {

        //引擎状态
        if (!state.compareAndSet(Reader.State.STOPPED, Reader.State.RUNNING)) {
            logger.info("引擎已经在执行！");
            return;
        }

        Filters filters = getAllFilters(config);

        //初始化mysql日志任务上下文Configuration
        this.mysqlTaskContext = createAndStartTaskContext(config, filters);
        this.mysqlTaskContext.getSource().setCompletedGtidSet(config.getString("gtid"));
        //mysql连接的上下文
        this.mysqlJdbcContext = mysqlTaskContext.getConnectionContext();

        final boolean isGtidModeEnabled = this.mysqlJdbcContext.isGtidModeEnabled();

        final SourceInfo source = mysqlTaskContext.getSource();


        String showMasterStmt = "SHOW MASTER STATUS";
        if (offsets != null && offsets.size() != 0) {
            //构建sourceinfo的offsets信息
            source.setoffset(offsets);
        } else {
            //原子读写
            AtomicReference<String> sql = new AtomicReference<String>(showMasterStmt);
            JdbcConnection mysql = mysqlJdbcContext.jdbc();
            try {
                mysql.query(sql.get(), rs -> {
                    if (rs.next()) {
                        String binlogFilename = rs.getString(1);
                        long binlogPosition = rs.getLong(2);
                        source.setBinlogStartPoint(binlogFilename, binlogPosition);
                        if(isGtidModeEnabled){
                            String executed_Gtid_Set = rs.getString(5);
                            source.setCompletedGtidSet(executed_Gtid_Set);
                        }
                    }
                });
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }


        String earliestBinlogFilename = earliestBinlogFilename();

        //暂时不支持快照，目前只要streaming方式接口。
        //目前业务场景也不需要快照
        if (earliestBinlogFilename == null) {
            logger.warn("确认mysql是否开启binlog");
        }
        //ROW模式
        final boolean binlogFormatRow = isBinlogFormatRow();

        //binlog日志记录所有前镜像和后镜像
        final boolean binlogRowImageFull = isBinlogRowImageFull();

        final boolean rowBinlogEnabled = binlogFormatRow && binlogRowImageFull;

        if (!rowBinlogEnabled) {
            if (!binlogFormatRow) {
                throw new ConnectException("The MySQL server is not configured to use a ROW binlog_format, which is "
                        + "required for this connector to work properly. Change the MySQL configuration to use a "
                        + "binlog_format=ROW and restart the connector.");
            } else {
                throw new ConnectException("The MySQL server is not configured to use a FULL binlog_row_image, which is "
                        + "required for this connector to work properly. Change the MySQL configuration to use a "
                        + "binlog_row_image=FULL and restart the connector.");
            }
        }

        //加载历史表结构
        mysqlTaskContext.loadHistory(source);

        //控制日志读取
        ChainedReader.Builder chainedReaderBuilder = new ChainedReader.Builder();

        //binlog日志
        BinlogReader binlogReader = new BinlogReader("binlog", mysqlTaskContext, null);

        chainedReaderBuilder.addReader(binlogReader);
        readers = chainedReaderBuilder.build();

        readers.uponCompletion(this::completeReaders);
        //初始化
        this.readers.initialize();

        //启动
        this.readers.start();
    }

    private Map<String, ?> getRestartOffset(Map<String, ?> storedOffset) {
        Map<String, Object> restartOffset = new HashMap<>();
        if (storedOffset != null) {
            for (Map.Entry<String, ?> entry : storedOffset.entrySet()) {
                if (entry.getKey().startsWith(SourceInfo.RESTART_PREFIX)) {
                    String newKey = entry.getKey().substring(SourceInfo.RESTART_PREFIX.length());
                    restartOffset.put(newKey, entry.getValue());
                }
            }
        }
        return restartOffset.isEmpty() ? storedOffset : restartOffset;
    }

    public List<BaseRecord> doPoll() throws InterruptedException {
        Reader currentReader = readers;
        if (currentReader == null) {
            return null;
        }
        return currentReader.poll();
    }

    @Override
    public List<BaseRecord> poll() throws InterruptedException {
        boolean started = startIfNeededAndPossible();

        //可重试异常还没到重试时间，等待
        if (!started) {
            Metronome.parker(Duration.of(2, ChronoUnit.SECONDS), Clock.SYSTEM).pause();
            return Collections.emptyList();
        }
        List<BaseRecord> records = null;
        try {
            return records = doPoll();
        } catch (InterruptedException e) {
            stop(true);
            throw e;
        }
    }

    /*
     * @Description:停止
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/19 11:33
     */
    protected void doStop() {

    }

    private void stop(boolean restart) {
        stateLock.lock();

        try {
            if (!state.compareAndSet(Reader.State.RUNNING, Reader.State.STOPPED)) {
                logger.info("Connector has already been stopped");
                return;
            }

            if (restart) {
                logger.warn("Going to restart connector after {} sec. after a retriable exception", retriableRestartWait.getSeconds());
            } else {
                logger.info("Stopping down connector");
            }


            doStop();

            if (restart && restartDelay == null) {
                restartDelay = ElapsedTimeStrategy.constant(Clock.system(), retriableRestartWait.toMillis());
                restartDelay.hasElapsed();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private boolean startIfNeededAndPossible() {
        stateLock.lock();

        try {
            if (state.get() == Reader.State.RUNNING) {
                return true;
            } else if (restartDelay != null && restartDelay.hasElapsed()) {
                start(config, offsets);
                return true;
            } else {
                logger.info("出现可重试错误等待中~~~");
                return false;
            }
        } finally {
            stateLock.unlock();
        }
    }

    public MySqlTaskContext getMysqlTaskContext() {
        return mysqlTaskContext;
    }

    private static Filters getOldFilters(Map<String, ?> offsets, Configuration config) {
        return new Filters.Builder(config).setFiltersFromOffsets(offsets).build();
    }

    private static MySqlTaskContext createAndStartTaskContext(Configuration config, Filters filters) {
        MySqlTaskContext taskContext = new MySqlTaskContext(config, filters);
        taskContext.start();
        return taskContext;
    }


    /*
     * @Description:
     * @Param:得到当前mysql最早可用的binlog名称
     * @return: java.lang.String
     * @Author: miaoneng
     * @Date: 2021/11/12 20:35
     */
    protected String earliestBinlogFilename() throws ConnectException {

        List<String> logNames = new ArrayList<>();
        try {
            logger.info("Checking all known binlogs from MySQL");
            mysqlJdbcContext.jdbc().query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        if (logNames.isEmpty())
            return null;
        return logNames.get(0);
    }

    /*
     * @Description:
     * STATEMENT模式（SBR）:每一条会修改数据的sql语句会记录到binlog中。优点是并不需要记录每一条sql语句和每一行的数据变化，减少了binlog日志量，节约IO，提高性能。缺点是在某些情况下会导致master-slave中的数据不一致(如sleep()函数， last_insert_id()，以及user-defined functions(udf)等会出现问题)
     * ROW模式（RBR）:不记录每条sql语句的上下文信息，仅需记录哪条数据被修改了，修改成什么样了。而且不会出现某些特定情况下的存储过程、或function、或trigger的调用和触发无法被正确复制的问题。缺点是会产生大量的日志，尤其是alter table的时候会让日志暴涨。
     * MIXED模式（MBR）:以上两种模式的混合使用，一般的复制使用STATEMENT模式保存binlog，对于STATEMENT模式无法复制的操作使用ROW模式保存binlog，MySQL会根据执行的SQL语句选择日志保存方式。
     * @Param:
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/9/8 13:51
     */
    protected boolean isBinlogFormatRow() throws ConnectException {
        AtomicReference<String> mode = new AtomicReference<String>("");
        try {
            mysqlJdbcContext.jdbc().query("SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> {
                if (rs.next()) {
                    mode.set(rs.getString(2));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at BINLOG mode: ", e);
        }
        logger.debug("binlog_format={}", mode.get());
        return "ROW".equalsIgnoreCase(mode.get());
    }

    private static Filters getAllFilters(Configuration config) {
        return new Filters.Builder(config).build();
    }

    /**
     * 确定 MySQL服务器是否将 binlog_row_image 设置为 "FULL"
     *
     * @return {@code true} if the server's {@code binlog_row_image} is set to {@code FULL}, or {@code false} otherwise
     */
    protected boolean isBinlogRowImageFull() {
        AtomicReference<String> rowImage = new AtomicReference<String>("");
        try {
            mysqlJdbcContext.jdbc().query("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'", rs -> {
                if (rs.next()) {
                    rowImage.set(rs.getString(2));
                } else {
                    // This setting was introduced in MySQL 5.6+ with default of 'FULL'.
                    // For older versions, assume 'FULL'.
                    rowImage.set("FULL");
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at BINLOG_ROW_IMAGE mode: ", e);
        }

        logger.debug("binlog_row_image={}", rowImage.get());

        return "FULL".equalsIgnoreCase(rowImage.get());
    }

    /*
     * @Description:读取binlog任务结束后执行所有收尾工作
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/18 17:05
     */
    protected void completeReaders() {

    }

}
