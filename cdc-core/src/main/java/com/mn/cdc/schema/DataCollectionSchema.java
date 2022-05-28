/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.schema;

import com.mn.cdc.data.Envelope;
import com.mn.cdc.structure.Schema;

/**
 * @program:cdc-master
 * @description 数据库的架构接口。提供有关它包含的表（集合等）结构的信息
 * @author:miaoneng
 * @create:2021-09-14 16:38
 **/
public interface DataCollectionSchema {

    DataCollectionId id();
    Schema keySchema();
    Envelope getEnvelopeSchema();
}
