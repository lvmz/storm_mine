package com.jk.storm_stat.jkss.areaStat_dev.bolt_port;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jk.storm_stat.util.UtilTools;
import com.jk.storm_stat.util.redisUtil.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Created by lmz on 2017/7/19.
 */
public class CacltorPortBolt extends BaseRichBolt {
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
        try {
            String userid = input.getStringByField("userid");
            String useend = input.getStringByField("useend");
            String userip = input.getStringByField("userip");
            String createtime_key = input.getStringByField("createtime_key");

            // 汇总各个访客端对应的 UV 数
            String key_uv = createtime_key +  "_" + useend + "_portuv";
            if (jedis.exists(key_uv)) {
                jedis.sadd(key_uv, userid);
            } else {
                jedis.sadd(key_uv, userid);
                jedis.expireAt(key_uv, UtilTools.getUnixTime(2));
            }
            // 汇总各个访客端对应的 PV 数
            String key_pv = createtime_key + "_" + useend + "_portpv";
            if (jedis.exists(key_pv)) {
                jedis.incr(key_pv);
            } else {
                jedis.incr(key_pv);
                jedis.expireAt(key_pv, UtilTools.getUnixTime(2));
            }
            //汇总各个访客端对应的 独立IP 数
            String key_ip = createtime_key + "_" + useend + "_portip";
            if (jedis.exists(key_ip)) {
                jedis.sadd(key_ip, userip);
            } else {
                jedis.sadd(key_ip, userip);
                jedis.expireAt(key_ip, UtilTools.getUnixTime(2));
            }
            this.collector.emit(new Values(useend , createtime_key));
            jedis.close();
            this.collector.ack(input);
        }catch (Exception e) {
            jedis.close();
            this.collector.fail(input);
            ERRORDATALOG.error(CacltorPortBolt.class.getCanonicalName() + "|exception:" +e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("useend", "createtime_key"));
    }
}
