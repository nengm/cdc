package com.mn.cdc.relational;

import com.mn.cdc.data.Envelope;
import com.mn.cdc.schema.DataCollectionSchema;
import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * @program:cdc-master
 * @description 表结构
 * @author:miaoneng
 * @create:2021-09-09 22:01
 **/
public class TableSchema implements DataCollectionSchema {
    private static final Logger logger = LoggerFactory.getLogger(TableSchema.class);
    private final TableId id;
    private final Schema keySchema;
    private final Envelope envelopeSchema;
    private final Schema valueSchema;
    public boolean isDeleted = false;

    //主键生成器
    private final Function<Object[], Struct> keyGenerator;

    //值生成器
    private final Function<Object[], Struct> valueGenerator;

    public TableSchema(TableId id, Schema keySchema,Function<Object[], Struct> keyGenerator,
                       Envelope envelopeSchema, Schema valueSchema, Function<Object[], Struct> valueGenerator) {
        this.id = id;
        this.keySchema = keySchema;
        this.envelopeSchema = envelopeSchema;
        this.valueSchema = valueSchema;

        this.keyGenerator = keyGenerator != null ? keyGenerator : (row) -> null;
        this.valueGenerator = valueGenerator != null ? valueGenerator : (row) -> null;
    }
    public TableId id() {
        return id;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    /*
     * @Description:将指定行的名称转换为值
     * @Param: columnData
     * @return: java.lang.Object
     * @Author: miaoneng
     * @Date: 2021/11/29 20:20
     */
    public Struct keyFromColumnData(Object[] columnData) {
        return columnData == null||isDeleted ? null : keyGenerator.apply(columnData);
    }

    /*
     * @Description:将行的值转换为固定的struct，struct可以还原出数据
     * @Param: columnData
     * @return: com.mn.cdc.structure.Struct
     * @Author: miaoneng
     * @Date: 2021/11/29 20:19
     */
    public Struct valueFromColumnData(Object[] columnData) {
        return columnData == null||isDeleted ? null : valueGenerator.apply(columnData);
    }
}
