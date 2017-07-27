package com.jk.storm_stat.util.redisUtil;

//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import redis.clients.jedis.Jedis;

public class RedisUtil1 {
	
	/**
     * 向Set集合添加元素
     */
//    public static void addToSet(String key, String value){
//    	Jedis jedis = RedisPool.getJedis();
//        jedis.sadd(key, value);
//        RedisPool.returnResource(jedis);
//    }
//    
//    /**
//     * 获取set集合大小
//     * @param key
//     * @return
//     */
//    public static Long getSetSize(String key){
//    	Jedis jedis = RedisPool.getJedis();
//    	Long value = jedis.scard(key);
//    	RedisPool.returnResource(jedis);
//    	return value;
//    }
//    
//    /**
//     * 移除集合元素
//     * @param key
//     * @param value
//     * @return
//     */
//    public static Long removeSetMem(String key, String value){
//    	Jedis jedis = RedisPool1.getJedis();
//    	Long value1 = jedis.srem("user","are");
//    	RedisPool1.returnResource(jedis);
//    	return value1;
//    }
//    
//    /**
//     * 区域
//     */
//    /**
//	 * String类型，value自增
//	 * @param key
//	 * @return
//	 */
//	public static Long incr(String key){
//		Jedis jedis = RedisPool.getJedis();
//		Long value = jedis.incr(key);
//		RedisPool.returnResource(jedis);
//		return value;
//	}
//	
//	/**
//	 * String 类型，根据key获取values
//	 * @param key
//	 * @return
//	 */
//	public static String get(String key){
//		Jedis jedis = RedisPool.getJedis();
//		String value = jedis.get(key);
//		RedisPool.returnResource(jedis);
//		return value;
//	}
//
//	/**
//	 * 判断 key 是否存在
//	 * @param key
//	 * @return
//	 */
//	public static boolean exists(String key){
//		Jedis jedis = RedisPool.getJedis();
//		boolean result = jedis.exists(key);
//		RedisPool.returnResource(jedis);
//		return result;
//	}
//
//
//	/**
//	 * String 类型，根据key设置value
//	 * @param key
//	 * @param value
//	 * @return
//	 */
//	public static String set(String key, Object value){
//		Jedis jedis = RedisPool.getJedis();
//		String result = jedis.set(key, value.toString());
//		RedisPool.returnResource(jedis);
//		return result;
//	}
//
//	/**
//     * String类型，设置 key 在某个 时间戳过期。
//	 * @param key
//     * @param unixTime
//     * @return
//     */
//	public static Long expireAt(String key, long unixTime){
//		Jedis jedis = RedisPool.getJedis();
//		Long result = jedis.expireAt(key, unixTime);
//		RedisPool.returnResource(jedis);
//		return result;
//	}
//
//
//	/**
//	 * 删除 key
//	 * @param key
//	 * @return
//	 */
//	public static Long del(String key){
//		Jedis jedis = RedisPool.getJedis();
//		Long result = jedis.del(key);
//		RedisPool.returnResource(jedis);
//		return result;
//	}
//
//	/**
//	 * 根据 pattern 通配符，返回查询列表
//	 * @param pattern
//	 * @return
//	 */
//	public static Set<String> keys(String pattern){
//		Jedis jedis = RedisPool.getJedis();
//		Set<String> keys = jedis.keys(pattern);
//		RedisPool.returnResource(jedis);
//		return keys;
//	}
//
//
//	/**
//	 * Hash类型，获取指定 key field 的 value
//	 * @param key
//	 * @param field
//	 * @return
//	 */
//	public static String hget(String key, String field){
//		Jedis jedis = RedisPool.getJedis();
//		String value = jedis.hget(key, field);
//		RedisPool.returnResource(jedis);
//		return  value;
//	}

	////////////////////////test///////////////////////////
    /**
     * 排序
     */
//    public static void test(){
//        jedis.del("number");//先删除数据，再进行测试
//        jedis.rpush("number","4");//将一个或多个值插入到列表的尾部(最右边)
//        jedis.rpush("number","5");
//        jedis.rpush("number","3");
//
//        jedis.lpush("number","9");//将一个或多个值插入到列表头部
//        jedis.lpush("number","1");
//        jedis.lpush("number","2");
//        System.out.println(jedis.lrange("number",0,jedis.llen("number")));
//        System.out.println("排序:"+jedis.sort("number"));
//        System.out.println(jedis.lrange("number",0,-1));//不改变原来的排序
//        jedis.del("number");//测试完删除数据
//    }
//    
//    
//    /**
//     * Redis操作字符串
//     */
//    public static void testString() {
//        //添加数据
//        jedis.set("name", "chx"); //key为name放入value值为chx
//        System.out.println("拼接前:" + jedis.get("name"));//读取key为name的值
//
//        //向key为name的值后面加上数据 ---拼接
//        jedis.append("name", " is my name;");
//        System.out.println("拼接后:" + jedis.get("name"));
//
//        //删除某个键值对
//        jedis.del("name");
//        System.out.println("删除后:" + jedis.get("name"));
//
//        //s设置多个键值对
//        jedis.mset("name", "chenhaoxiang", "age", "20", "email", "chxpostbox@outlook.com");
//        jedis.incr("age");//用于将键的整数值递增1。如果键不存在，则在执行操作之前将其设置为0。 如果键包含错误类型的值或包含无法表示为整数的字符串，则会返回错误。此操作限于64位有符号整数。
//        System.out.println(jedis.get("name") + " " + jedis.get("age") + " " + jedis.get("email"));
//    }
//
//    public static void testMap() {
//        //添加数据
//        Map<String, String> map = new HashMap<String, String>();
//        map.put("name", "chx");
//        map.put("age", "100");
//        map.put("email", "***@outlook.com");
//        jedis.hmset("user", map);
//        //取出user中的name，结果是一个泛型的List
//        //第一个参数是存入redis中map对象的key，后面跟的是放入map中的对象的key，后面的key是可变参数
//        List<String> list = jedis.hmget("user", "name", "age", "email");
//        System.out.println(list);
//
//        //删除map中的某个键值
//        jedis.hdel("user", "age");
//        System.out.println("age:" + jedis.hmget("user", "age")); //因为删除了，所以返回的是null
//        System.out.println("user的键中存放的值的个数:" + jedis.hlen("user")); //返回key为user的键中存放的值的个数2
//        System.out.println("是否存在key为user的记录:" + jedis.exists("user"));//是否存在key为user的记录 返回true
//        System.out.println("user对象中的所有key:" + jedis.hkeys("user"));//返回user对象中的所有key
//        System.out.println("user对象中的所有value:" + jedis.hvals("user"));//返回map对象中的所有value
//
//        //拿到key，再通过迭代器得到值
//        Iterator<String> iterator = jedis.hkeys("user").iterator();
//        while (iterator.hasNext()) {
//            String key = iterator.next();
//            System.out.println(key + ":" + jedis.hmget("user", key));
//        }
//        jedis.del("user");
//        System.out.println("删除后是否存在key为user的记录:" + jedis.exists("user"));//是否存在key为user的记录
//
//    }
//
//    /**
//     * jedis操作List
//     */
//    public static void testList(){
//        //移除javaFramwork所所有内容
//        jedis.del("javaFramwork");
//        //存放数据
//        jedis.lpush("javaFramework","spring");
//        jedis.lpush("javaFramework","springMVC");
//        jedis.lpush("javaFramework","mybatis");
//        //取出所有数据,jedis.lrange是按范围取出
//        //第一个是key，第二个是起始位置，第三个是结束位置
//        System.out.println("长度:"+jedis.llen("javaFramework"));
//        //jedis.llen获取长度，-1表示取得所有
//        System.out.println("javaFramework:"+jedis.lrange("javaFramework",0,-1));
//
//        jedis.del("javaFramework");
//        System.out.println("删除后长度:"+jedis.llen("javaFramework"));
//        System.out.println(jedis.lrange("javaFramework",0,-1));
//    }
}
