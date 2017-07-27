package com.jk.storm_stat.util;

import java.io.IOException;
import java.util.Properties;

/**
 * redis配置信息
 *
 */
public class Redissource {
	static Properties pro = null;
	static{
		pro = new Properties();
		try {
			pro.load(Redissource.class.getClassLoader().getResourceAsStream("redis.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int redis_maxIdle = Integer.parseInt(pro.getProperty("redis.pool.maxIdle"));
	public static int redis_maxTotal = Integer.parseInt(pro.getProperty("redis.pool.maxTotal"));
	public static int redis_maxWait = Integer.parseInt(pro.getProperty("redis.pool.maxWait"));
	public static Boolean redis_testOnBorrow = Boolean.parseBoolean(pro.getProperty("redis.pool.testOnBorrow"));
	public static Boolean redis_testONReturn = Boolean.parseBoolean(pro.getProperty("redis.pool.testOnReturn"));
	public static String redis_host = pro.getProperty("redis.host");
	public static int redis_port = Integer.parseInt(pro.getProperty("redis.port"));
	public static int redis_timeout = Integer.parseInt(pro.getProperty("redis.timeout"));

}
