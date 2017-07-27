package com.jk.storm_stat.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaSource {
	public static final String KAFKA_TOPIC_WX;
	public static final String KAFKA_TOPIC_TEACHER_WEB;
	public static final String KAFKA_TOPIC_TEACHER_APP;
	public static final String KAFKA_TOPIC_TEACHER_PC;
	public static final String KAFKA_TOPIC_REPORT;
	public static final String KAFKA_TOPIC_STUDENT_APP;
	public static final String KAFKA_TOPIC_STUDENT_WEB;
	public static final String KAFKA_ZKHOSTS;
	public static final String KAFKA_ZKROOT;
	
	public static final String GROUP_QUOTA_WX;
	public static final String GROUP_QUOTA_ST_APP;
	public static final String GROUP_QUOTA_ST_WEB;
	public static final String GROUP_QUOTA_TE_WEB;
	public static final String GROUP_QUOTA_TE_APP;
	public static final String GROUP_QUOTA_TE_PC;
	public static final String GROUP_QUOTA_RE;
	
	public static final String ZKPORT;
	public static final String ZKNODE5;
	public static final String ZKNODE6;
	public static final String ZKNODE7;
//	public static final String ZKNODE1;
//	public static final String ZKNODE2;
//	public static final String ZKNODE15;
//	public static final String ZKNODE16;
//	public static final String ZKNODE17;
	public static final String MODE;

	static {
		Properties pro = new Properties();
		ClassLoader classLoader = KafkaSource.class.getClassLoader();
		InputStream resourceAsStream = classLoader.getResourceAsStream("kafka.properties");
		try {
			pro.load(resourceAsStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		KAFKA_TOPIC_WX = pro.getProperty("KAFKA_TOPIC_WX");
		KAFKA_TOPIC_TEACHER_WEB = pro.getProperty("KAFKA_TOPIC_TEACHER_WEB");
		KAFKA_TOPIC_TEACHER_APP = pro.getProperty("KAFKA_TOPIC_TEACHER_APP");
		KAFKA_TOPIC_TEACHER_PC = pro.getProperty("KAFKA_TOPIC_TEACHER_PC");
		KAFKA_TOPIC_REPORT = pro.getProperty("KAFKA_TOPIC_REPORT");
		KAFKA_TOPIC_STUDENT_APP = pro.getProperty("KAFKA_TOPIC_STUDENT_APP");
		KAFKA_TOPIC_STUDENT_WEB = pro.getProperty("KAFKA_TOPIC_STUDENT_WEB");

		KAFKA_ZKHOSTS = pro.getProperty("KAFKA_ZKHOSTS");
		KAFKA_ZKROOT = pro.getProperty("KAFKA_ZKROOT");
		
		GROUP_QUOTA_WX = pro.getProperty("group_quota_wx");
		GROUP_QUOTA_ST_APP = pro.getProperty("group_quota_st_app");
		GROUP_QUOTA_ST_WEB = pro.getProperty("group_quota_st_web");
		GROUP_QUOTA_TE_WEB = pro.getProperty("group_quota_te_web");
		GROUP_QUOTA_TE_APP = pro.getProperty("group_quota_te_app");
		GROUP_QUOTA_TE_PC = pro.getProperty("group_quota_te_pc");
		GROUP_QUOTA_RE = pro.getProperty("group_quota_re");
		
		ZKPORT = pro.getProperty("zkPort");
		ZKNODE5 = pro.getProperty("zkNode5");
		ZKNODE6 = pro.getProperty("zkNode6");
		ZKNODE7 = pro.getProperty("zkNode7");
		
//		ZKNODE1 = pro.getProperty("zkNode1");
//		ZKNODE2 = pro.getProperty("zkNode2");
//		ZKNODE15 = pro.getProperty("zkNode15");
//		ZKNODE16 = pro.getProperty("zkNode16");
//		ZKNODE17 = pro.getProperty("zkNode17");
		
		MODE = pro.getProperty("mode");
	}
}
