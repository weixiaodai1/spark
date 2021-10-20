package com.example.spark_cloud.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUtil {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static volatile JedisPool jedisPool = null;

    public JedisPoolUtil() {
    }

    public static JedisPool getJedisPoolInstance() {
        if (jedisPool == null) {
            Class var0 = JedisPoolUtil.class;
            synchronized(JedisPoolUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(1000);
                    poolConfig.setMaxIdle(32);
                    poolConfig.setMaxWaitMillis(100000L);
                    poolConfig.setTestOnBorrow(true);
                    jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379);
                }
            }
        }

        return jedisPool;
    }

    public static Jedis getJedisInstance() {
        return getJedisPoolInstance().getResource();
    }

    public static void release(JedisPool jedisPool, Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResourceObject(jedis);
        }

    }
}
