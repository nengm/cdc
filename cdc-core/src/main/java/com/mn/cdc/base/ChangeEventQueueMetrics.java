package com.mn.cdc.base;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-08 16:04
 **/
public interface ChangeEventQueueMetrics {
    int totalCapacity();

    int remainingCapacity();

    long maxQueueSizeInBytes();

    long currentQueueSizeInBytes();
}
