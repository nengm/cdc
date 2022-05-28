package com.mn.cdc.schema;

/**
 * @program:cdc-master
 * @description 当前连接配置实例如何处理filter的工厂
 * @author:miaoneng
 * @create:2021-09-30 15:58
 **/
public interface DataCollectionFilters {

    public DataCollectionFilter<?> dataCollectionFilter();

    @FunctionalInterface
    public interface DataCollectionFilter<T>{
        boolean isIncluded(T id);
    }
}
