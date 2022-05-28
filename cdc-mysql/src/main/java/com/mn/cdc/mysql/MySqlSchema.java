package com.mn.cdc.mysql;

import com.mn.cdc.config.CommonEngineConfig.*;
import com.mn.cdc.config.Configuration;
import com.mn.cdc.document.Document;
import com.mn.cdc.exception.CdcException;
import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import com.mn.cdc.jdbc.JdbcValueConverters.DecimalMode;
import com.mn.cdc.jdbc.TemporalPrecisionMode;
import com.mn.cdc.mysql.antlr.MySqlAntlrDdlParser;
import com.mn.cdc.mysql.config.MySqlEngineConfig;
import com.mn.cdc.mysql.config.MySqlEngineConfig.*;
import com.mn.cdc.relational.*;
import com.mn.cdc.relational.ddl.DdlChanges;
import com.mn.cdc.relational.ddl.DdlParser;
import com.mn.cdc.relational.history.DatabaseHistory;
import com.mn.cdc.relational.history.DatabaseHistoryMetrics;
import com.mn.cdc.relational.history.FileDatabaseHistory;
import com.mn.cdc.relational.history.HistoryRecordComparator;
import com.mn.cdc.schema.SchemaChangeEvent;
import com.mn.cdc.schema.SchemaChangeEvent.*;
import com.mn.cdc.text.MultipleParsingExceptions;
import com.mn.cdc.text.ParsingException;
import com.mn.cdc.util.SchemaNameAdjuster;
import com.mn.cdc.util.Strings;
import com.mn.cdc.relational.ddl.DdlParserListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * @program:cdc-master
 * @description mysql数据库的组织和结构
 * @author:miaoneng
 * @create:2021-09-11 10:23
 **/
public class MySqlSchema extends RelationalDatabaseSchema {
    private final static Logger logger = LoggerFactory.getLogger(MySqlSchema.class);

    //database,table,column等的正则匹配
    private final Filters filters;

    //ddl解析接口
    private final DdlParser ddlParser;

    //DDL变化记录
    private final DdlChanges ddlChanges;

    //是否跳过不能转换的语句
    private final boolean skipUnparseableDDL;

    //是否只存观察的表，其他表的表结构不存
    private final boolean storeOnlyCapturedTablesDdl;

    //历史修改比较器，比较哪个修改在前，哪个在后
    private final HistoryRecordComparator historyComparator;

    //是否恢复了表
    private boolean recoverdTables;

    String DB_HISTORY_PATH = "E:\\work\\gitlab\\tmp\\test2.txt";
    private final Map<Long, TableId> tableIdsByTableNumber = new ConcurrentHashMap<>();

    private final Set<String> ignoredQueryStatements = new HashSet<String>() {
        {
            add("BEGIN");
            add("END");
            add("FLUSH PRIVILEGES");
        }
    };

    //历史数据结构记录(慢慢加入)
    private final DatabaseHistory dbHistory;

    MySqlJdbcContext mysqlJdbcContext;
    MySqlTaskContext mysqlTaskContext;


    public MySqlSchema(MySqlJdbcContext mysqlJdbcContext, MySqlTaskContext mysqlTaskContext, MySqlEngineConfig mysqlEngineConfig,
                       Predicate<String> gtidFilter, boolean tableIdCaseInsensitive, Filters tableFilters) {
        super(mysqlEngineConfig, Tables.TableFilter.fromPredicate(tableFilters.tableFilter()), tableFilters.columnFilter(), new TableSchemaBuilder(getValueConverters(mysqlEngineConfig)
                , SchemaNameAdjuster.create(), SourceInfo.SCHEMA), true);
        this.mysqlJdbcContext = mysqlJdbcContext;
        this.mysqlTaskContext = mysqlTaskContext;
        this.filters = tableFilters;

        //跳过不能解析的语句
        this.skipUnparseableDDL = true;

        //只保存监控的表
        this.storeOnlyCapturedTablesDdl = true;

        //ddl解析接口
        this.ddlParser = new MySqlAntlrDdlParser(getValueConverters(mysqlEngineConfig), getTableFilter());

        //ddl变化记录
        this.ddlChanges = this.ddlParser.getDdlChanges();



        //构建数据库监控表的表结构历史记录
        Configuration config = Configuration.create()
                                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                                .build();
        this.dbHistory = config.getInstance(MySqlEngineConfig.DATABASE_HISTORY, FileDatabaseHistory.class);

        if (this.dbHistory == null) {
            throw new ConnectException("无法实例化DatabaseHistory" +
                    config.getString(MySqlEngineConfig.DATABASE_HISTORY));
        }

        //比较位点函数，这样可以知道我哪个位点数据用哪个数据结构解析
        this.historyComparator = new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return SourceInfo.isPositionAtOrBefore(recorded, desired, gtidFilter);
            }
        };
        this.dbHistory.configure(config,historyComparator,new DatabaseHistoryMetrics(mysqlEngineConfig),true);

    }
    private TableId getTableId(DdlParserListener.Event event) {
        if (event instanceof DdlParserListener.TableEvent) {
            return ((DdlParserListener.TableEvent) event).tableId();
        }
        else if (event instanceof DdlParserListener.TableIndexEvent) {
            return ((DdlParserListener.TableIndexEvent) event).tableId();
        }
        return null;
    }

    private static MySqlValueConverters getValueConverters(MySqlEngineConfig configuration) {
        // 使用mysql特定的转换器和模式得到值

        //设置使用的时间为adaptive_time_microseconds（TIME字段总是使用INT64表示微妙）
        TemporalPrecisionMode timePrecisionMode = configuration.getTemporalPrecisionMode();

        //默认二进制表示decimal，更加精准
        DecimalMode decimalMode = configuration.getDecimalMode();

        //默认long表示bigint
        String bigIntUnsignedHandlingModeStr = configuration.getConfig().getString(MySqlEngineConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode = BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        //默认启用自动时间调整
        final boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlEngineConfig.ENABLE_TIME_ADJUSTER);

        // TODO mysql日志分析错误给出二进制坐标
        return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                configuration.binaryHandlingMode(), timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                (message, exception) -> {
                    if (configuration
                            .getEventProcessingFailureHandlingMode() == EventProcessingFailureHandlingMode.FAIL) {
                        throw new CdcException(message, exception);
                    }
                    else if (configuration
                            .getEventProcessingFailureHandlingMode() == EventProcessingFailureHandlingMode.WARN) {
                        logger.warn(message, exception);
                    }
                });
    }

    /*
     * @Description:开启持久化数据库数据结构变更
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/11/26 13:50
     */
    public synchronized void start(){
        this.dbHistory.start();
    }

    /*
     * @Description:关闭持久化数据库数据结构变更
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/11/26 13:51
     */
    public synchronized void shutdown(){
        this.dbHistory.stop();
    }
    /*
     * @Description:拿到表结构，由于有的时候建表语句没有拿到，因为有可能是建表后再监听的，所以这块表结构定义就没了
     *              只能扫描一次表拿到。
     * @Param: id
     * @return: com.mn.cdc.relational.TableSchema
     * @Author: miaoneng
     * @Date: 2021/9/14 19:08
     */
    @Override
    public TableSchema schemaFor(TableId id) {

        return super.schemaFor(id);
    }
    protected String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    protected String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }


    public boolean applyDdl(SourceInfo sourceInfo, String databaseName, String ddlStatements, DdlChanges.DatabaseStatementStringConsumer statementConsumer) {
        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);
        Set<TableId> changes;
        if (ignoredQueryStatements.contains(ddlStatements)) {
            return false;
        }
        try {
            this.ddlChanges.reset();
            this.ddlParser.setCurrentSchema(databaseName);
            this.ddlParser.parse(ddlStatements, getTables());
        } catch (ParsingException | MultipleParsingExceptions e) {
            if (skipUnparseableDDL) {
                logger.warn("Ignoring unparseable DDL statement '{}': {}", ddlStatements, e);
            } else {
                throw e;
            }
        }
        changes = getTables().drainChanges();
        // No need to send schema events or store DDL if no table has changed
        if (!storeOnlyCapturedTablesDdl || ddlChanges.anyMatch(filters.databaseFilter(), filters.tableFilter())) {
            if (statementConsumer != null) {

                // We are supposed to _also_ record the schema changes as SourceRecords, but these need to be filtered
                // by database. Unfortunately, the databaseName on the event might not be the same database as that
                // being modified by the DDL statements (since the DDL statements can have fully-qualified names).
                // Therefore, we have to look at each statement to figure out which database it applies and then
                // record the DDL statements (still in the same order) to those databases.

                if (!ddlChanges.isEmpty() && ddlChanges.applyToMoreDatabasesThan(databaseName)) {

                    // We understood at least some of the DDL statements and can figure out to which database they apply.
                    // They also apply to more databases than 'databaseName', so we need to apply the DDL statements in
                    // the same order they were read for each _affected_ database, grouped together if multiple apply
                    // to the same _affected_ database...
                    ddlChanges.groupStatementStringsByDatabase((dbName, tables, ddl) -> {
                        if (filters.databaseFilter().test(dbName) || dbName == null || "".equals(dbName)) {
                            if (dbName == null) {
                                dbName = "";
                            }
                            statementConsumer.consume(dbName, tables, ddl);
                        }
                    });
                }
                else if (filters.databaseFilter().test(databaseName) || databaseName == null || "".equals(databaseName)) {
                    if (databaseName == null) {
                        databaseName = "";
                    }
                    statementConsumer.consume(databaseName, changes, ddlStatements);
                }
            }

            // 记录DDL语句，以便我们以后可以在需要时恢复。模式改变的时候记录，以后如果需要恢复，就不会丢掉这块模式的信息了。
            // 只记录下面3种信息
            // - all DDLs if configured
            // - or global SET variables
            // - or DDLs for monitored objects
            if (!storeOnlyCapturedTablesDdl || isGlobalSetVariableStatement(ddlStatements, databaseName) || changes.stream().anyMatch(filters().tableFilter()::test)) {
                dbHistory.record(sourceInfo.partition(), sourceInfo.offset(), databaseName, ddlStatements);
            }
        } else {
            logger.info("DDL '{}' 的修改不会记录在历史记录中", ddlStatements);
        }
        changes.forEach(tableId -> {
            Table table = tableFor(tableId);
            if (table == null) {
                removeSchema(tableId);
            }
            else {
                buildAndRegisterSchema(table);
            }
        });


        return true;
    }
    private void emitChangeEvent(SourceInfo sourceInfo, List<SchemaChangeEvent> schemaChangeEvents,
                                 final String sanitizedDbName, DdlParserListener.Event event, TableId tableId, SchemaChangeEventType type) {
        schemaChangeEvents.add(new SchemaChangeEvent(sourceInfo.struct(),
                sanitizedDbName, null, event.statement(), tableId != null ? tableFor(tableId) : null, type));
    }

    /*
     * @Description:是否捕捉此database的元数据更改
     * @Param: databaseName
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/11/25 15:23
     */
    private boolean acceptableDatabase(final String databaseName) {
        return filters.databaseFilter().test(databaseName)
                || databaseName == null
                || databaseName.isEmpty();
    }
    public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
        return databaseName == null && ddl != null && ddl.toUpperCase().startsWith("SET ");
    }

    /*
     * @Description:是否为给定标捕获事件
     * @Param: id 表的完全限定标识符，不能为空
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/9/28 11:27
     */
    public boolean isTableMonitored(TableId id) {
        return filters.tableFilter().test(id);
    }

    public boolean skipSchemaChangeEvent(SchemaChangeEvent event) {
        if (!Strings.isNullOrEmpty(event.getDatabase())
                && !mysqlTaskContext.getEngineConfig().getTableFilters().databaseFilter().test(event.getDatabase())) {
            logger.debug("Skipping schema event as it belongs to a non-captured database: '{}'", event);
            return true;
        }
        return false;
    }

    /*
     * @Description:数据库的表过滤器
     * @Param:null
     * @return: com.mn.cdc.mysql.Filters
     * @Author: miaoneng
     * @Date: 2021/11/26 16:49
     */
    public Filters filters() {
        return filters;
    }

    public void setSystemVariables(Map<String,String> variables){
        variables.forEach((varName,value)->{
            ddlParser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION,varName,value);
        });
    }

    public void loadHistory(SourceInfo startingPoint){
        getTables().clear();
        dbHistory.recover(startingPoint.partition(),startingPoint.offset(),getTables(),ddlParser);
        recoverdTables = !tableIds().isEmpty();
        refreshSchemas();
    }
    public void refreshSchemas(){
        clearSchemas();
        this.tableIds().forEach(id->{
            Table table = this.tableFor(id);
            buildAndRegisterSchema(table);
        });
    }
}
