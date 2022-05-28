package com.mn.cdc.source;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-29 16:10
 **/

import java.util.Map;

/**
 * SourceTaskContext is provided to SourceTasks to allow them to interact with the underlying
 * runtime.
 */
public interface SourceTaskContext {
    /**
     * Get the Task configuration.  This is the latest configuration and may differ from that passed on startup.
     *
     * For example, this method can be used to obtain the latest configuration if an external secret has changed,
     * and the configuration is using variable references such as those compatible with
     */
    public Map<String, String> configs();

    /**
     * Get the OffsetStorageReader for this SourceTask.
     */
    OffsetStorageReader offsetStorageReader();
}
