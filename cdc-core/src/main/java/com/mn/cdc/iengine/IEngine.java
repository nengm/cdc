package com.mn.cdc.iengine;

import com.mn.cdc.config.Configuration;
import com.mn.cdc.structure.BaseRecord;
import com.mn.cdc.structure.SourceRecord;

import java.util.List;
import java.util.Map;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-10-19 14:47
 **/
public interface IEngine {

    /*
     * @Description:启动引擎
     * @Param: config
     * @param: offsets
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/10/20 11:06
     */
    public void start(Configuration config, Map<String, ?> offsets);

    /*
     * @Description:提取引擎拿到的数据
     * @Param:
     * @return: java.util.List<com.mn.cdc.structure.SourceRecord>
     * @Author: miaoneng
     * @Date: 2021/10/20 11:07
     */
    public List<BaseRecord> poll() throws InterruptedException;

}
