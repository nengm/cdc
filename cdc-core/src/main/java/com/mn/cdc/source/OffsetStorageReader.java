package com.mn.cdc.source;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-29 16:07
 **/
import java.util.Collection;
import java.util.Map;

/*
 * @Description:OffsetStorageReader 提供对源使用的偏移存储的访问,设置偏移然后访问,这在任务初始化期间最常用，但也可以在运行时使用，例如重新配置任务时
 * @Param: null
 * @return:
 * @Author: miaoneng
 * @Date: 2021/9/29 16:07
 */
public interface OffsetStorageReader {
    /**
     * Get the offset for the specified partition. If the data isn't already available locally, this
     * gets it from the backing store, which may require some network round trips.
     *
     * @param partition object uniquely identifying the partition of data
     * @return object uniquely identifying the offset in the partition of data
     */
    <T> Map<String, Object> offset(Map<String, T> partition);

    /**
     * <p>
     * Get a set of offsets for the specified partition identifiers. This may be more efficient
     * than calling {@link #offset(Map)} repeatedly.
     * </p>
     * <p>
     * Note that when errors occur, this method omits the associated data and tries to return as
     * many of the requested values as possible. This allows a task that's managing many partitions to
     * still proceed with any available data. Therefore, implementations should take care to check
     * that the data is actually available in the returned response. The only case when an
     * exception will be thrown is if the entire request failed, e.g. because the underlying
     * storage was unavailable.
     * </p>
     *
     * @param partitions set of identifiers for partitions of data
     * @return a map of partition identifiers to decoded offsets
     */
    <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions);
}

