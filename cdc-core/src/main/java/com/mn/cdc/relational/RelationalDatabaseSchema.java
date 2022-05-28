package com.mn.cdc.relational;

import com.mn.cdc.config.CommonEngineConfig;
import com.mn.cdc.mapping.ColumnMappers;
import com.mn.cdc.schema.DatabaseSchema;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;


/**
 * @program:cdc-master
 * @description 关系型数据的物理结构和cdc的消息结构
 * @author:miaoneng
 * @create:2021-09-09 21:00
 **/
public class RelationalDatabaseSchema implements DatabaseSchema<TableId> {
    private final TableSchemaBuilder schemaBuilder;
    private Tables.TableFilter tableFilter;
    private final Predicate<ColumnId> columnFilter;

    //通过tableId找到表结构schema
    private final SchemasByTableId schemasByTableId;
    private final Tables tables;
    private final ColumnMappers columnMappers;
    private final String schemaPrefix;

    protected RelationalDatabaseSchema(CommonEngineConfig commonConfig, Tables.TableFilter tableFilter, Predicate<ColumnId> columnFilter, TableSchemaBuilder tableSchemaBuilder, boolean tableIdCaseInsensitive) {
        this.schemaBuilder = tableSchemaBuilder;
        this.tableFilter = tableFilter;
        this.columnFilter = columnFilter;
        this.schemasByTableId = new SchemasByTableId(tableIdCaseInsensitive);
        this.tables = new Tables(tableIdCaseInsensitive);
        this.columnMappers =  ColumnMappers.create(commonConfig.getConfig());
        this.schemaPrefix = getSchemaPrefix(commonConfig.getLogicalName());
    }

    /*
     * @Description:key：tableId value:schema,通过tableId找到表结构
     * @Param: null
     * @return:
     * @Author: miaoneng
     * @Date: 2021/9/11 19:33
     */
    private static class SchemasByTableId {

        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, TableSchema> values;

        public SchemasByTableId(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new ConcurrentHashMap<>();
        }

        public void clear() {
            values.clear();
        }

        public TableSchema remove(TableId tableId) {
            return values.remove(toLowerCaseIfNeeded(tableId));
        }

        public TableSchema get(TableId tableId) {
            return values.get(toLowerCaseIfNeeded(tableId));
        }

        public TableSchema put(TableId tableId, TableSchema updated) {
            return values.put(toLowerCaseIfNeeded(tableId), updated);
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public TableSchema schemaFor(TableId id) {
        return schemasByTableId.get(id);
    }

    public Tables getTables() {
        return tables;
    }

    /*
     * @Description:根据表的标识得到表的元数据，考虑到筛选条件
     * @Param: id
     * @return: com.mn.cdc.relational.Table
     * @Author: miaoneng
     * @Date: 2021/9/18 11:34
     */
    public Table tableFor(TableId id) {
        return tableFilter.isIncluded(id) ? tables.forTable(id) : null;
    }

    protected void removeSchema(TableId id) {
        schemasByTableId.remove(id);
    }


    /*通过cdc 给定表的结构改变事件把记录到schema中
     * @Description:
     * @Param: table
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/18 11:41
     */
    protected void buildAndRegisterSchema(Table table) {
        if (tableFilter.isIncluded(table.id())) {
            TableSchema schema = schemaBuilder.create(schemaPrefix, "",table, columnFilter, columnMappers);
            schemasByTableId.put(table.id(), schema);
        }
    }
    private static String getSchemaPrefix(String serverName) {
        if (serverName == null) {
            return "";
        }
        else {
            serverName = serverName.trim();
            return serverName.endsWith(".") || serverName.isEmpty() ? serverName : serverName + ".";
        }
    }
    protected Tables.TableFilter getTableFilter() {
        return tableFilter;
    }

    public Set<TableId> tableIds(){
        //返回监控的表的表id
        return tables.subset(tableFilter).tableIds();
    }
    protected void clearSchemas(){
        schemasByTableId.clear();
    }
}
