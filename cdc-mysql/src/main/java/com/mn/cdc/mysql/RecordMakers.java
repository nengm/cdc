package com.mn.cdc.mysql;

import com.mn.cdc.data.Envelope;
import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.function.BlockingConsumer;
import com.mn.cdc.relational.Table;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.relational.TableSchema;
import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.SchemaBuilder;
import com.mn.cdc.structure.SourceRecord;
import com.mn.cdc.structure.Struct;
import com.mn.cdc.relational.history.HistoryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program:cdc-master
 * @description 生成SourceRecord的组件
 * @author:miaoneng
 * @create:2021-09-09 13:36
 **/
public class RecordMakers {
    private final Logger logger = LoggerFactory.getLogger(RecordMakers.class);
    private final SourceInfo sourceInfo;


    private final Map<Long, Converter> convertersByTableNumber = new HashMap<>();
    private final Map<TableId, Long> tableNumbersByTableId = new HashMap<>();
    private final Map<Long, TableId> tableIdsByTableNumber = new HashMap<>();

    private final Map<String, ?> restartOffset;

    //数据库建表语句结构
    private final MySqlSchema schema;

    private final Schema schemaChangeKeySchema;
    private final Schema schemaChangeValueSchema;

    public RecordMakers(MySqlSchema schema, SourceInfo sourceInfo, Map<String, ?> restartOffset) {
        this.schema = schema;
        this.sourceInfo = sourceInfo;
        this.restartOffset = restartOffset;

        this.schemaChangeKeySchema = SchemaBuilder.struct()
                .name("connector.mysql.SchemaChangeKey")
                .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA).build();

        this.schemaChangeValueSchema = SchemaBuilder.struct()
                .name("connector.mysql.SchemaChangeValue")
                .field(HistoryRecord.Fields.SOURCE, SourceInfo.SCHEMA).field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.STRING_SCHEMA).build();

    }

    protected static interface Converter {
        int read(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts,
                 BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

        int insert(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts,
                   BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

        int update(SourceInfo source, Object[] before, Object[] after, int rowNumber, int numberOfRows,
                   BitSet includedColumns, Instant ts, BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

        int delete(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts,
                   BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

    }

    /*
     * @Description:关联table number与表，构建相应表的元数据
     * @Param: tableNumber
     * @param: id
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/9/9 14:26
     */
    public boolean assign(long tableNumber, TableId id) {
        Long existingTableNumber = tableNumbersByTableId.get(id);
        if (existingTableNumber != null && existingTableNumber.longValue() == tableNumber && convertersByTableNumber.containsKey(tableNumber)) {
            return true;
        }
        TableSchema tableSchema = schema.schemaFor(id);
        if (tableSchema == null) {
            return false;
        }
        Envelope envelope = tableSchema.getEnvelopeSchema();

        Converter converter = new Converter() {
            private void validateColumnCount(TableSchema tableSchema, Object[] row) {
                final int expectedColumnsCount = schema.tableFor(tableSchema.id()).columns().size();
                if (expectedColumnsCount != row.length) {
                    logger.error("Invalid number of columns, expected '{}' arrived '{}'", expectedColumnsCount, row.length);
                    throw new ConnectException(
                            "The binlog event does not contain expected number of columns; the internal schema representation is probably out of sync with the real database schema, or the binlog contains events recorded with binlog_row_image other than FULL or the table in question is an NDB table");
                }
            }
            @Override
            public int read(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
                Struct key = tableSchema.keyFromColumnData(row);
                Struct value = tableSchema.valueFromColumnData(row);
                if (value != null || key != null) {
                    Schema keySchema = tableSchema.keySchema();
                    Map<String, ?> partition = source.partition();
                    Map<String, Object> offset = source.offsetForRow(rowNumber, numberOfRows);
                    Struct origin = source.struct(id);
                    SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), keySchema, key, envelope.schema(), envelope.read(value, origin, ts));
                    consumer.accept(record);
                    return 1;
                }
                return 0;
            }

            @Override
            public int insert(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
                validateColumnCount(tableSchema, row);
                Struct key = tableSchema.keyFromColumnData(row);
                Struct value = tableSchema.valueFromColumnData(row);
                if (value != null || key != null) {
                    Schema keySchema = tableSchema.keySchema();
                    Map<String, ?> partition = source.partition();
                    Map<String, Object> offset = source.offsetForRow(rowNumber, numberOfRows);
                    source.tableEvent(id);
                    Struct origin = source.struct();
                    SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), keySchema, key, envelope.schema(), envelope.create(value, origin, ts));
                    consumer.accept(record);
                    return 1;
                }
                return 0;
            }

            @Override
            public int update(SourceInfo source, Object[] before, Object[] after, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
                int count =0;
                Struct key = tableSchema.keyFromColumnData(after);
                Struct valueAfter = tableSchema.valueFromColumnData(after);
                if(valueAfter != null || key != null){
                    Object oldKey = tableSchema.keyFromColumnData(before);
                    Object valueBefore = tableSchema.valueFromColumnData(before);
                    Schema keySchema = tableSchema.keySchema();
                    Map<String ,?> partition = source.partition();
                    Map<String , Object> offset = source.offsetForRow(rowNumber,numberOfRows);
                    Struct origin = source.struct(id);

                    //主键改变比较麻烦，自己权衡如何消费数据处理
                    if(key != null && !Objects.equals(key,oldKey)){
                        //主键改变，必须所有以前的主键和新的主键
                        //删除旧键结构
                        SourceRecord record = new SourceRecord(partition,getSourceRecordOffset(offset),keySchema,oldKey,envelope.schema(),envelope.delete(valueBefore,origin,ts));
                        consumer.accept(record);

                        //发出旧值
                        record = new SourceRecord(partition, getSourceRecordOffset(offset),keySchema, oldKey, null, null);
                        consumer.accept(record);
                        ++count;

                        //发出create事件
                        record = new SourceRecord(partition, getSourceRecordOffset(offset), keySchema, key, envelope.schema(), envelope.create(valueAfter, origin, ts));
                        consumer.accept(record);
                        ++count;
                    }else{
                        //主键没有更新，直接发送update事件
                        SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), keySchema, key, envelope.schema(),
                                envelope.update(valueBefore, valueAfter, origin, ts));
                        consumer.accept(record);
                        ++count;
                    }
                }
                return count;
            }

            @Override
            public int delete(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, Instant ts, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
                int count =0;
                Struct key = tableSchema.keyFromColumnData(row);
                Struct value = tableSchema.valueFromColumnData(row);
                if(value != null||key!=null){
                    Schema keySchema = tableSchema.keySchema();
                    Map<String ,?> partition = source.partition();
                    Map<String ,Object> offset = source.offsetForRow(rowNumber,numberOfRows);
                    Struct origin = source.struct(id);

                    //发出删除事件
                    SourceRecord record = new SourceRecord(partition,getSourceRecordOffset(offset),keySchema,key,envelope.schema(),envelope.delete(value,origin,ts));
                    consumer.accept(record);
                    ++count;
                }
                return 0;
            }
        };
        //存入tableNumber对应的转换
        convertersByTableNumber.put(tableNumber,converter);

        //存入id对应的tableNumber
        Long previousTableNumber = tableNumbersByTableId.put(id,tableNumber);

        //存入tableNumber对应的id
        tableIdsByTableNumber.put(tableNumber,id);

        //如果以前相应的tableNumber，说明以前的转换存在，删除旧的转换
        if(previousTableNumber != null){
            assert previousTableNumber.longValue() != tableNumber;
            convertersByTableNumber.remove(previousTableNumber);
        }


        return true;

    }
    private Map<String, ?> getSourceRecordOffset(Map<String, Object> sourceOffset) {
        if (restartOffset == null) {
            return sourceOffset;
        }
        else {
            for (Map.Entry<String, ?> restartOffsetEntry : restartOffset.entrySet()) {
                sourceOffset.put(SourceInfo.RESTART_PREFIX + restartOffsetEntry.getKey(), restartOffsetEntry.getValue());
            }

            return sourceOffset;
        }
    }
    /*
     * @Description:ddl语句生成数据库组织结构改变的记录
     * @Param: databaseName 发出ddl语句的数据库，不能为空
     * @param: ddlStatements ddl语句，不能为空
     * @param: consumer 记录消费者，不能为空
     * @return: int 产生记录条数
     * @Author: miaoneng
     * @Date: 2021/9/21 9:09
     */
    public int schemaChanges(String databaseName, Set<TableId> tables, String ddlStatements, BlockingConsumer<SourceRecord> consumer) {
        String topicName = databaseName;
        Integer partition = 0;
        Struct key = schemaChangeRecordKey(databaseName);
        Struct value = schemaChangeRecordValue(databaseName, tables, ddlStatements);
        SourceRecord record = new SourceRecord(sourceInfo.partition(), sourceInfo.offset(),schemaChangeKeySchema, key, schemaChangeValueSchema, value);
        try {
            consumer.accept(record);
            return 1;
        } catch (InterruptedException e) {
            return 0;
        }
    }

    protected Struct schemaChangeRecordKey(String databaseName) {
        Struct result = new Struct(schemaChangeKeySchema);
        result.put("databaseName", databaseName);
        return result;
    }

    protected Struct schemaChangeRecordValue(String databaseName, Set<TableId> tables, String ddlStatements) {
        sourceInfo.databaseEvent(databaseName);
        sourceInfo.tableEvent(tables);
        Struct result = new Struct(schemaChangeValueSchema);
        result.put("source", sourceInfo.struct());
        result.put("databaseName", databaseName);
        result.put("ddl", ddlStatements);
        return result;
    }

    /*
     * @Description:清理所有缓存，日志切换后，所有表会用新的编号
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/9/9 14:04
     */
    public void clear() {
        logger.debug("清除缓存");
        convertersByTableNumber.clear();
        tableNumbersByTableId.clear();
        tableIdsByTableNumber.clear();
    }

    /**
     * 根据给定表获得记录生成器，通过记录生成器把记录发给指定的表
     *
     * @param tableNumber     the {@link #assign(long, TableId) assigned table number} for which records are to be produced
     * @param includedColumns the set of columns that will be included in each row; may be null if all columns are included
     * @param consumer        the consumer for all produced records; may not be null
     * @return the table-specific record maker; may be null if the table is not included in the connector
     */
    public RecordsForTable forTable(long tableNumber, BitSet includedColumns, BlockingConsumer<SourceRecord> consumer) {
        Converter converter = convertersByTableNumber.get(tableNumber);
        if (converter == null)
            return null;
        return new RecordsForTable(converter, includedColumns, consumer);
    }

    /**
     * 将tablenum转为tableid，tableid中包含更多的信息
     *
     * @param tableNumber
     * @return the table id or null for unknown tables
     */
    public TableId getTableIdFromTableNumber(long tableNumber) {
        return tableIdsByTableNumber.get(tableNumber);
    }

    public void regenerate() {
        clear();
        AtomicInteger nextTableNumber = new AtomicInteger(0);
        Set<TableId> tableIds = schema.tableIds();
        logger.info("为表重新生成转换器");
        tableIds.forEach(id -> {
            assign(nextTableNumber.incrementAndGet(), id);
        });
    }

    /**
     * {@link SourceRecord} 特定表的处理函数的工厂类
     */
    public final class RecordsForTable {
        private final BitSet includedColumns;
        private final Converter converter;
        private final BlockingConsumer<SourceRecord> consumer;

        protected RecordsForTable(Converter converter, BitSet includedColumns,
                                  BlockingConsumer<SourceRecord> consumer) {
            this.converter = converter;
            this.includedColumns = includedColumns;
            this.consumer = consumer;
        }

        /**
         * Produce a {@link Envelope.Operation#READ read} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts  the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int read(Object[] row, Instant ts) throws InterruptedException {
            return read(row, ts, 0, 1);
        }

        /**
         * Produce a {@link Envelope.Operation#READ read} record for the row.
         *
         * @param row          the values of the row, in the same order as the columns in the {@link Table} definition in the
         *                     {@link MySqlSchema}.
         * @param ts           the timestamp for this row
         * @param rowNumber    the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int read(Object[] row, Instant ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.read(sourceInfo, row, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }

        /**
         * Produce a {@link Envelope.Operation#CREATE create} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts  the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int create(Object[] row, Instant ts) throws InterruptedException {
            return create(row, ts, 0, 1);
        }

        /**
         * Produce a {@link Envelope.Operation#CREATE create} record for the row.
         *
         * @param row          the values of the row, in the same order as the columns in the {@link Table} definition in the
         *                     {@link MySqlSchema}.
         * @param ts           the timestamp for this row
         * @param rowNumber    the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int create(Object[] row, Instant ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.insert(sourceInfo, row, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }

        /**
         * Produce an {@link Envelope.Operation#UPDATE update} record for the row.
         *
         * @param before the values of the row <i>before</i> the update, in the same order as the columns in the {@link Table}
         *               definition in the {@link MySqlSchema}
         * @param after  the values of the row <i>after</i> the update, in the same order as the columns in the {@link Table}
         *               definition in the {@link MySqlSchema}
         * @param ts     the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int update(Object[] before, Object[] after, Instant ts) throws InterruptedException {
            return update(before, after, ts, 0, 1);
        }

        /**
         * Produce an {@link Envelope.Operation#UPDATE update} record for the row.
         *
         * @param before       the values of the row <i>before</i> the update, in the same order as the columns in the {@link Table}
         *                     definition in the {@link MySqlSchema}
         * @param after        the values of the row <i>after</i> the update, in the same order as the columns in the {@link Table}
         *                     definition in the {@link MySqlSchema}
         * @param ts           the timestamp for this row
         * @param rowNumber    the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int update(Object[] before, Object[] after, Instant ts, int rowNumber, int numberOfRows)
                throws InterruptedException {
            return converter.update(sourceInfo, before, after, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }

        /**
         * Produce a {@link Envelope.Operation#DELETE delete} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts  the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int delete(Object[] row, Instant ts) throws InterruptedException {
            return delete(row, ts, 0, 1);
        }

        /**
         * Produce a {@link Envelope.Operation#DELETE delete} record for the row.
         *
         * @param row          the values of the row, in the same order as the columns in the {@link Table} definition in the
         *                     {@link MySqlSchema}.
         * @param ts           the timestamp for this row
         * @param rowNumber    the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int delete(Object[] row, Instant ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.delete(sourceInfo, row, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }
    }
}