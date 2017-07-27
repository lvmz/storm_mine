package com.jk.storm_stat.jkss.areaStat_dev.bolt_area;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jk.storm_stat.util.TransJson;
import com.jk.storm_stat.util.redisUtil.RedisPool;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by lmz on 2017/7/18.
 */
public class FormatAreaBolt extends BaseRichBolt{

    private static final long serialVersionUID = 1L;
    private static final Logger WARNDATELOG = LoggerFactory.getLogger("warnMessage");
    private static final Logger ERRORDATALOG = LoggerFactory.getLogger("errorMessage");

    OutputCollector collector;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    JedisPool jedisPool = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.jedisPool = RedisPool.getPool();
    }

    @Override
    public void execute(Tuple input) {

        String message = (String)input.getValueByField("message");

        Jedis jedis = this.jedisPool.getResource();
        try {
            //转json，根据key获取value
            JSONObject log = JSONObject.fromObject(message);
            JSONObject json = new JSONObject();
            TransJson.flattenJson(log, json);
//          暂时默认key一定含有以下4个。 不做判断： json.containsKey("userid");
            String userid = json.getString("userid");
            String allUseend = json.getString("useend");
            String useend = null;
            if(allUseend.equals("15")){
                useend = "15" ;
            }else {
                useend = allUseend.substring(0, 1);
            }
            String userip = json.getString("userip");
            String createtime = json.getString("createtime");
            String city_code =  null;
            Object position = json.get("position");
            //区域位置不为空
            if (null != position && !"null".equals(position.toString())) {
                //取省市代码
                String province_city = position.toString().substring(0, position.toString().indexOf("市") + 1).replaceAll("中国", "");
                //hget返回hash表中指定字段的值！
                city_code = jedis.hget("province_city", province_city);
                ERRORDATALOG.error(FormatAreaBolt.class.getCanonicalName() + "|puhaha|" +  message);

                if(StringUtils.isNotBlank(city_code)){
                    //取年月日
                    String createtime_key = sdf.format(new Date(sdf.parse(createtime).getTime())).replaceAll("-", "");
                    ERRORDATALOG.error(FormatAreaBolt.class.getCanonicalName() + "|xixi|" +  message);
                    //发送
                    this.collector.emit(new Values(userid, useend, userip, createtime_key, city_code));
                    ERRORDATALOG.error(FormatAreaBolt.class.getCanonicalName() + "|ixix|" +  message);
                }
            } else {
                //异常数据处理
                WARNDATELOG.warn(FormatAreaBolt.class.getCanonicalName() + "|position is null:" + message);
            }
            //释放资源
            jedis.close();
            //标记接收完成
            this.collector.ack(input);
        } catch (Exception e) {
            jedis.close();
            this.collector.fail(input);
            ERRORDATALOG.error(FormatAreaBolt.class.getCanonicalName() + "|" + e.getMessage() + ":" + message);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("userid", "useend", "userip", "createtime_key", "city_code"));
    }
}
