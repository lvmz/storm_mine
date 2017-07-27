package com.jk.storm_stat.jkss.areaStat_dev.bolt_area;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.jk.storm_stat.util.UtilTools;
import com.jk.storm_stat.util.redisUtil.RedisPool;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * 进行单线程汇总最终结果
 *
 * @author shenfl
 */
public class CacltorAreaBolt extends BaseRichBolt {

    private static final Logger WARNDATELOG = LoggerFactory.getLogger("warnMessage");
    private static final Logger ERRORDATALOG = LoggerFactory.getLogger("errorMessage");
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    JedisPool jedisPool;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.jedisPool = RedisPool.getPool();
    }

    @Override
    public void execute(Tuple input) {
        Jedis jedis = this.jedisPool.getResource();
        try{
            String userid = input.getStringByField("userid");
            String useend = input.getStringByField("useend");
            String userip = input.getStringByField("userip");
            String city_code = input.getStringByField("city_code");
            String createtime_key = input.getStringByField("createtime_key");
            //判断区域代码
            if(StringUtils.isNotBlank(city_code)){
                // 汇总各个访客端对应的 UV 数
                String key_uv = createtime_key + "_" + city_code + "_" + useend + "_uv";
                if (jedis.exists(key_uv)) {
                    jedis.sadd(key_uv, userid);
                }else{
                    jedis.sadd(key_uv, userid);
                    jedis.expireAt(key_uv, UtilTools.getUnixTime(2));
                }
                // 汇总各个访客端对应的 UV 数
                String key_pv = createtime_key + "_" + city_code + "_" + useend + "_pv";
                if (jedis.exists(key_pv)) {
                    jedis.incr(key_pv);
                }else{
                    jedis.incr(key_pv);
                    jedis.expireAt(key_pv, UtilTools.getUnixTime(2));
                }
                //汇总各个访客端对应的 独立IP 数
                String key_ip = createtime_key + "_" + city_code + "_" + useend + "_ip";
                if(jedis.exists(key_ip)){
                    jedis.sadd(key_ip, userip);
                }else{
                    jedis.sadd(key_ip, userip);
                    jedis.expireAt(key_ip, UtilTools.getUnixTime(2));
                }
                this.collector.emit(new Values(useend, city_code, createtime_key));
                WARNDATELOG.warn(CacltorAreaBolt.class.getCanonicalName() + "|==========meme==========:" + city_code);

            }else{
                //异常数据处理
                WARNDATELOG.warn(CacltorAreaBolt.class.getCanonicalName() + "|city_code is null:" + city_code);
            }
            jedis.close();
            this.collector.ack(input);
        }catch (Exception e) {
            jedis.close();
            this.collector.fail(input);
            ERRORDATALOG.error(CacltorAreaBolt.class.getCanonicalName() + "|exception:" +e.getMessage());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("useend", "city_code", "createtime_key"));
    }

}