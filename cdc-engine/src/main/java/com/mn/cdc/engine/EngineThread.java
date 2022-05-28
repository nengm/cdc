package com.mn.cdc.engine;

import com.alibaba.fastjson.JSONObject;
import com.mn.cdc.config.Configuration;
import com.mn.cdc.config.Field;
import com.mn.cdc.iengine.IEngine;
import com.mn.cdc.structure.BaseRecord;
import com.mn.util.redis.RedisWhiteList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @program:cdc-master
 * @description 启动线程
 * @author:miaoneng
 * @create:2021-09-29 17:38
 **/
public class EngineThread implements Runnable, Closeable {
    private final Logger logger = LoggerFactory.getLogger(EngineThread.class);
    private final Configuration config;

    private final AtomicReference<Thread> runningThread = new AtomicReference<>();

    public final Field ENGINE_CLASS = Field.create("engine.class").withDescription("引擎全限定类名").required();
    public EngineThread(Configuration config){
        this.config = config;
    }
    @Override
    public void run() {

        if(runningThread.compareAndSet(null,Thread.currentThread())){
            final String engineClassName = config.getString(ENGINE_CLASS);
            RedisWhiteList.init();
            try {
                ClassLoader classLoader = getClass().getClassLoader();
                Class<? extends IEngine> engineClass =(Class<? extends IEngine>)classLoader.loadClass(engineClassName);
                IEngine iEngine = engineClass.getDeclaredConstructor().newInstance();
                String position = config.getString("position");
                iEngine.start(config,getStratOffset(position));
                while(runningThread.get() != null){
                    List<BaseRecord> changeRecords = null;
                    try{

                        //logger.info("线程："+runningThread.get().getId()+" 正在提取数据");
                        changeRecords = iEngine.poll();
                        if(changeRecords != null && changeRecords.size() != 0){
                            logger.info("############{}",changeRecords.toString());

                            //这边就可以做你的操作了
                            //进行事务塞入kafka队列，或者入库，同时修改位点。
                            //建议oracle同一个位点的数据一起事务塞入，如果挂了后，读取当前位点，然后位点+1后进行读取，这样内部就会少n多计算。
                        }
                        logger.info("线程："+runningThread.get().getId()+" 提取到 "+changeRecords.size() + "条数据");

                    }catch (InterruptedException e){
                        logger.info(e.toString());
                    }

                }

            } catch (Throwable t) {
                t.printStackTrace();
            }finally {
                runningThread.set(null);
                //完成

            }
        }

    }

    @Override
    public void close() throws IOException {

    }

    /*
     * @Description:直接传入pos
     * @Param: pos
     * @return: java.util.Map<java.lang.String,java.lang.Object>
     * @Author: miaoneng
     * @Date: 2021/9/30 10:03
     */
    public Map<String,Object> getStratOffset(String pos){
        HashMap<String,Object> map = new HashMap<>();
        JSONObject jsonObject = JSONObject.parseObject(pos);
        if(jsonObject == null){
            return null;
        }
        return jsonObject.getInnerMap();
    }

}
