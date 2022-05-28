package com.mn.cdc.schema;

/**
 * @program:cdc-master
 * @description 数据库的架构。提供有关它包含的表（集合等）结构的信息
 * @author:miaoneng
 * @create:2021-09-14 16:08
 **/
public interface DatabaseSchema<I>{
    void close();
    DataCollectionSchema schemaFor(I id);
}

