package com.example.spark_cloud.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestRedisPool {
    private JedisPool pool = null;

    public TestRedisPool(String ip, int port, String passwd, int database) {
        if (this.pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(500);
            config.setMaxIdle(30);
            config.setMinIdle(5);
            config.setMaxWaitMillis(10000L);
            config.setTestWhileIdle(false);
            config.setTestOnBorrow(false);
            config.setTestOnReturn(false);
            this.pool = new JedisPool(config, ip, port, 10000, passwd);
            System.out.println("init:" + this.pool);
        }

    }

    public JedisPool getRedisPool() {
        return this.pool;
    }

    public String getRedisPoolByKey(String key) {
        Jedis jedis = this.pool.getResource();
        return jedis.get(key);
    }

    public String set(String key, String value) {
        Jedis jedis = null;

        String var5;
        try {
            jedis = this.pool.getResource();
            String var4 = jedis.set(key, value);
            return var4;
        } catch (Exception var9) {
            var9.printStackTrace();
            var5 = "0";
        } finally {
            jedis.close();
        }

        return var5;
    }
}
