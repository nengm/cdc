package com.mn.util.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-11-24 14:53
 **/
public class RedisWhiteList {

    private static Logger logger = LoggerFactory.getLogger(RedisUtil.class);
    public static ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> whilteListMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String ,String > tableWhiteListMapString = new ConcurrentHashMap<>();
    static {
        tableWhiteListMapString.put("CDC_MysqlWhiteList","");
        tableWhiteListMapString.put("CDC_OracleWhiteList","");
        tableWhiteListMapString.put("CDC_SqlServerWhiteList","");

        whilteListMap.put("CDC_MysqlWhiteList",new ConcurrentLinkedQueue<>());
        whilteListMap.put("CDC_OracleWhiteList",new ConcurrentLinkedQueue<>());
        whilteListMap.put("CDC_SqlServerWhiteList",new ConcurrentLinkedQueue<>());
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                for(String watchList : tableWhiteListMapString.keySet()){
                    String redisTableWhitelist = RedisUtil.getInstance().get(watchList);
                    //没redis可以直接去掉redis，然后这边使用下面的字符串替换
                    //String redisTableWhitelist = "miaon.baseinfo,miaon.account";
                    if(redisTableWhitelist == null){
                        continue;
                    }
                    String oldTableWhiteList = tableWhiteListMapString.get(watchList);
                    if(!redisTableWhitelist.equals(oldTableWhiteList)){
                        //当前白名单
                        String [] tablWhiteStrings = redisTableWhitelist.split(",");

                        //上一次白名单
                        String [] lastTableWhiteStrings = oldTableWhiteList.split(",");

                        Set<String> set = new HashSet<String>(Arrays.asList(tablWhiteStrings));
                        Set<String> set_last = new HashSet<String>(Arrays.asList(lastTableWhiteStrings));

                        for(String table : set) {
                            if(!set_last.contains(table)) {
                                whilteListMap.get(watchList).add(table);
                            }
                        }
                        for(String table : set_last) {
                            if(!set.contains(table)) {
                                whilteListMap.get(watchList).remove(table);
                            }
                        }
                        tableWhiteListMapString.put(watchList,redisTableWhitelist) ;
                    }

                }
                logger.info("whilteListMap:{}",whilteListMap);

            }
        };
        Timer timer = new Timer();
        // 从现在开始每间隔 1000 ms 计划执行一个任务（规律性重复执行调度 TimerTask）
        timer.schedule(task, 0 ,5000);
    }
    public static void init(){
        logger.info("启动扫描白名单!");
    }
}
