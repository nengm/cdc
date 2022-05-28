package com.mn.util.redis;

import redis.clients.jedis.Jedis;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-11-22 14:48
 **/
public interface RedisExecutor <T>{
    T execute(Jedis jedis);
}
