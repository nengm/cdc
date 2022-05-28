package com.mn.cdc.mysql;

import com.mn.cdc.structure.SourceRecord;

/**
 * @program:cdc-master
 * @description 实现调用的谓词，以确定它们是否应该继续处理记录
 * @author:miaoneng
 * @create:2021-09-08 14:56
 **/
@FunctionalInterface
public interface HaltingPredicate {
    boolean accepts(SourceRecord record);
}
