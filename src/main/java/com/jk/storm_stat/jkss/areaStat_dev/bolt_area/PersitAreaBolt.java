package com.jk.storm_stat.jkss.areaStat_dev.bolt_area;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.jk.storm_stat.util.DruidDBPool;
import com.jk.storm_stat.util.PropertiesType;
import com.jk.storm_stat.util.redisUtil.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.*;
import java.util.Map;

/**
 * 进行单线程汇总最终结果
 *
 * @author shenfl
 */
public class PersitAreaBolt extends BaseRichBolt {

    private static final Logger ERRORDATALOG = LoggerFactory.getLogger("errorMessage");
    private static final long serialVersionUID = 1L;
    double emitFrequencyInSeconds = 0.2;
    OutputCollector collector;
    JedisPool jedisPool;
    DruidDBPool pool;
    Connection connection;
    PreparedStatement ps;
    PreparedStatement psI = null;
    PreparedStatement psU = null;
    PreparedStatement psQ = null;
    int count;
    ResultSet rs = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.jedisPool = RedisPool.getPool();
        this.pool = new DruidDBPool(PropertiesType.STORM_STAT_ONLINE);
        try {
            this.connection = this.pool.getConnection();
            //关闭自动提交事务
            this.connection.setAutoCommit(false);
            this.psQ = this.connection.prepareStatement("select count(1) from jike_calculate_area where useend = ? and city_code = ? and to_days(createtime) = to_days(now())");
            this.psI = this.connection.prepareStatement("insert into jike_calculate_area(userview, pageview, independip, useend, city_code, createtime) values(?,?,?,?,?,now())");
            this.psU = this.connection.prepareStatement("update jike_calculate_area set createtime = now(), userview = ?, pageview = ?, independip = ? WHERE useend = ? and city_code = ? and to_days(createtime) = to_days(now())");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        //保存到Mysql数据库中
        if (null == this.connection) {
            try {
                this.connection = this.pool.getConnection();
                //关闭自动提交事务
                this.connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //定时器执行
        if (isTickTuple(tuple)) {
            try {
//                System.out.println("======== tick start count = " + count + ":" + new Timestamp(System.currentTimeMillis()));
                if (count > 0) {
                    count = 0;
                    ps.executeBatch();
                    this.connection.commit();
                }
//                System.out.println("tick end :" + new Timestamp(System.currentTimeMillis()));
            } catch (Throwable e) {
                if (connection != null) {
                    try {
                        connection.rollback();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }
                }
                e.printStackTrace();
            }
            return;
        } else {
            Jedis jedis = this.jedisPool.getResource();
            try {
                String day = tuple.getStringByField("createtime_key");
                String useend = tuple.getStringByField("useend");
                String city_code = tuple.getStringByField("city_code");
                // 汇总各个访客端对应的 UV 数
                String key_uv = day + "_" + city_code + "_" + useend + "_uv";
                Long count_uv = jedis.scard(key_uv);
                // 汇总各个访客端对应的 PV 数
                String key_pv = day + "_" + city_code + "_" + useend + "_pv";
                Long count_pv = Long.parseLong(jedis.get(key_pv));
                // 汇总各个访客端对应的 独立IP 数
                String key_ip = day + "_" + city_code + "_" + useend + "_ip";
                Long count_ip = jedis.scard(key_ip);
                //保存到Mysql数据库中

                ps = this.psQ;
                ps.setInt((int) 1, Integer.parseInt(useend));
                ps.setLong((int) 2, Integer.parseInt(city_code));
                rs = ps.executeQuery();
                long result = 0;
                while (rs.next()) {
                    result = rs.getLong((int) 1);
                }
                if (result > 0) {
                    ps = this.psU;
                    ps.setLong((int) 1, count_uv);
                    ps.setLong((int) 2, count_pv);
                    ps.setLong((int) 3, count_ip);
                    ps.setInt((int) 4, Integer.parseInt(useend));
                    ps.setLong((int) 5, Integer.parseInt(city_code));
                    ps.addBatch();
                    this.count++;
                } else {
                    ps = this.psI;
                    ps.setLong((int) 1, count_uv);
                    ps.setLong((int) 2, count_pv);
                    ps.setLong((int) 3, count_ip);
                    ps.setInt((int) 4, Integer.parseInt(useend));
                    ps.setLong((int) 5, Integer.parseInt(city_code));
                    ps.executeUpdate();
                    this.connection.commit();
                    System.out.println("======== save insert into:" + new Timestamp(System.currentTimeMillis()));
                }

                //定时执行
                if (this.count >= 200) {
                    System.out.println("======== count save begin count = " + count + ":" + new Timestamp(System.currentTimeMillis()));
                    count = 0;
                    ps.executeBatch();
                    this.connection.commit();
                    System.out.println("count save end:" + new Timestamp(System.currentTimeMillis()));
                }

                //释放链接资源
                rs.close();
                jedis.close();
                this.collector.ack(tuple);

            } catch (Exception e) {
                jedis.close();
                this.collector.fail(tuple);
                ERRORDATALOG.error(e.getMessage());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

//    @Override
//    public Map<String, Object> getComponentConfiguration() {
//        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, ConfigUtils.TICK_TUPLE_FREQ_SECS);
//        return conf;
//    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
    public boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}