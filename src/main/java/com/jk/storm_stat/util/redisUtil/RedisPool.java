package com.jk.storm_stat.util.redisUtil;

import com.jk.storm_stat.util.Redissource;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**    
 * Redis操作接口 
 * 
 * @author 林计钦 
 * @version 1.0 2013-6-14 上午08:54:14    
 */  
public class RedisPool {  
    private static JedisPool pool = null;
      
    /** 
     * 构建redis连接池 
     */  
    public static JedisPool getPool() {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();  
            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；  
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。  
            //config.setMaxActive(500);
            config.setMaxTotal(Redissource.redis_maxTotal);
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。  
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；  
            //config.setMaxWait(1000 * 100);
            config.setMaxIdle(Redissource.redis_maxIdle);
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；  
            config.setTestOnBorrow(Redissource.redis_testOnBorrow);
            config.setTestOnReturn(Redissource.redis_testONReturn);
            config.setMaxWaitMillis(Redissource.redis_maxWait);
            
            pool = new JedisPool(config, Redissource.redis_host, Redissource.redis_port);
        }
        return pool;
    }  
      
    /** 
     * 返还到连接池 
     *  
     * @param pool  
     * @param redis 
     */  
    public static void returnResource(JedisPool pool, Jedis redis) {  
        if (redis != null) {  
            pool.returnResource(redis);  
        }  
    }  
      
}
