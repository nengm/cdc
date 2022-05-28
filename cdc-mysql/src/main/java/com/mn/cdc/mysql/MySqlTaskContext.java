package com.mn.cdc.mysql;

import com.mn.cdc.config.Configuration;
import com.mn.cdc.function.Predicates;
import com.mn.cdc.mysql.config.MySqlEngineConfig;
import com.mn.cdc.relational.history.DatabaseHistory;
import com.mn.cdc.util.Clock;
import com.mn.cdc.util.GtidSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;

/**
 * @program:cdc-master
 * @description mysql任务上下文
 * @author:miaoneng
 * @create:2021-09-07 14:27
 **/
public class MySqlTaskContext {
    private final Logger logger = LoggerFactory.getLogger(MySqlTaskContext.class);

    private final MySqlJdbcContext connectionContext;

    //当前mysql数据结构
    private final MySqlSchema mysqlSchema;

    private final SourceInfo sourceInfo;

    private final Configuration config;
    private final Clock clock;

    private final Predicate<String> gtidSourceFilter;

    private final Predicate<String> ddlFilter;

    private final MySqlEngineConfig mysqlEngineConfig;

    //解析后生成SourceRecord的组件
    private final RecordMakers recordMakers;
    public SourceInfo getSource() {
        return sourceInfo;
    }

    // lower_case_table_names：      此参数不可以动态修改，必须重启数据库
    // lower_case_table_names = 1    表名存储在磁盘是小写的，但是比较的时候是不区分大小写
    // lower_case_table_names = 0    表名存储为给定的大小和比较是区分大小写的
    // lower_case_table_names = 2    表名存储为给定的大小写但是比较的时候是小写的
    Boolean tableIdCaseInsensitive;

    public MySqlJdbcContext getConnectionContext() {
        return connectionContext;
    }
    public MySqlTaskContext(Configuration config, Filters filters)  {
        this(config, filters, null);
    }
    public MySqlTaskContext(Configuration config, Filters filters, Map<String, ?> restartOffset)  {
        this.config = config;
        //mysql引擎配置
        this.mysqlEngineConfig = new MySqlEngineConfig(config);

        //拿到mysql jdbc连接
        this.connectionContext = new MySqlJdbcContext(mysqlEngineConfig);

        //是否区分大小写
        this.tableIdCaseInsensitive = "1".equals(connectionContext.readMySqlSystemVariables().get("lower_case_table_names"));

        this.sourceInfo = new SourceInfo(mysqlEngineConfig);
        //服务名与线程号绑定
        this.sourceInfo.setServerName(String.valueOf(Thread.currentThread().getId()));
        this.clock = Clock.system();

        //下面两个参数没有特殊情况吗，一般用不到，用来筛选位置的
        //查找mysql服务器中的位置用的正则表达式，正则用逗号分隔，gtid中查找uuids。只有GTID列表包涵匹配的才会被使用到。不可与一起使用gtid.source.excludes。
        String gtidSetIncludes = config.getString("gtid.source.includes");
        //查找mysql服务器中的位置用的正则表达式，正则用逗号分隔，gtid中查找uuids。只有GTID列表不匹配的才会被使用到。不可与一起使用gtid.source.includes。
        String gtidSetExcludes =config.getString("gtid.source.excludes");

        this.gtidSourceFilter = gtidSetIncludes != null ? Predicates.includesUuids(gtidSetIncludes)
                : (gtidSetExcludes != null ? Predicates.excludesUuids(gtidSetExcludes) : null);

        //设置mysql架构
        this.mysqlSchema = new MySqlSchema(connectionContext, this, mysqlEngineConfig,this.gtidSourceFilter, this.tableIdCaseInsensitive, filters);

        //处理生成SourceRecord组件
        recordMakers = new RecordMakers(mysqlSchema,sourceInfo,restartOffset);

        //在binlog_format=row模式下，事务中create或drop临时表后，后面再执行DML（影响多行的DML)如果失败，那么失败的DML会回滚，但DML仍然记录了binlog。这个 binlog 应用到备库后会导致主备不一致。
        //过滤存入的dll语句
        String ddlFilter = config.getString(DatabaseHistory.DDL_FILTER);
        this.ddlFilter = Predicates.includes(ddlFilter);

    }
    public Clock getClock() {
        return clock;
    }
    public String username() {
        return config.getString(MySqlEngineConfig.USER);
    }

    public String password() {
        return config.getString(MySqlEngineConfig.PASSWORD);
    }

    public String hostname() {
        return config.getString(MySqlEngineConfig.HOSTNAME);
    }

    public int port() {
        return config.getInteger(MySqlEngineConfig.PORT);
    }

    public void start() {
        connectionContext.start();
        mysqlSchema.start();
    }

    public MySqlSchema mysqlSchema() {
        return mysqlSchema;
    }

    public RecordMakers makeRecord(){
        return recordMakers;
    }
    /**
     * Get the predicate function that will return {@code true} if a DDL has to be skipped over and left out of the schema history
     * or {@code false} when it should be processed.
     *
     * @return the DDL predicate function; never null
     */
    public Predicate<String> ddlFilter() {
        return ddlFilter;
    }


    public long serverId() {
        return config.getLong(MySqlEngineConfig.SERVER_ID);
    }

    public Configuration config() {
        return config;
    }

    public MySqlEngineConfig getEngineConfig() {
        return this.mysqlEngineConfig;
    }


    /*
     * @Description:得到slave需要的GTID
     * @Param: availableServerGtidSet mysql当前已经执行的GTID
     * @param: purgedServerGtid 目前slave已经执行的GITD，也就是这段GTID前的数据已经全部到达
     * @return: GtidSet slave需要的GTID
     * @Author: miaoneng
     * @Date: 2021/10/22 16:46
     */
    public GtidSet filterGtidSet(GtidSet availableServerGtidSet, GtidSet purgedServerGtid){
        String gtidStr = sourceInfo.gtidSet();
        if(gtidStr == null){
            return null;
        }
        logger.info("从先前记录的偏移量设置的GTID：",gtidStr);
        GtidSet filteredGtidSet = new GtidSet(gtidStr);
        GtidSet mergedGtidSet = availableServerGtidSet.with(filteredGtidSet);
        logger.info("最终合并的GTID设置给slave：{}",mergedGtidSet);
        return mergedGtidSet;
    }

    public boolean includeSchemaChangeRecords(){
        return config.getBoolean(MySqlEngineConfig.INCLUDE_SCHEMA_CHANGES);
    }


    /*
     * @Description:加载历史表结构变化，不允许数据默认的字符集改变。不考虑改变这种的情况！！！
     * @Param: startingPoint 起点
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/11/28 12:34
     */
    public void loadHistory(SourceInfo startingPoint){
        //加载mysql实例的默认字符集
        //character_set_server	utf8mb4
        //collation_server	    utf8mb4_general_ci
        Map<String,String > variables = connectionContext.readMySqlCharsetSystemVariables();
        mysqlSchema.setSystemVariables(variables);

        //加载历史数据结构
        mysqlSchema.loadHistory(startingPoint);

        recordMakers.regenerate();
    }
}
