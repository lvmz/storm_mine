package com.jk.storm_stat.jkss.areaStat_dev.bolt_port;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jk.storm_stat.util.TransJson;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by lmz on 2017/7/19.
 */
public class FormatPortBolt extends BaseRichBolt {

    OutputCollector collector;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final Logger WARNDATELOG = LoggerFactory.getLogger("warnMessage");
    private static final Logger ERRORDATALOG = LoggerFactory.getLogger("errorMessage");

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String message = (String) tuple.getValueByField("message");
        WARNDATELOG.info(FormatPortBolt.class.getCanonicalName() + "|==========?????==========:" + message);

        try {
            //转json，根据key获取value
            JSONObject log = JSONObject.fromObject(message);
            JSONObject json = new JSONObject();
            TransJson.flattenJson(log, json);
            String userid = json.getString("userid");
            String oruseend = json.getString("useend");
            String userip = json.getString("userip");
            String createtime = json.getString("createtime");
            String createtime_key = sdf.format(new Date(sdf.parse(createtime).getTime())).replaceAll("-", "");

            //日志信息校验
            if (StringUtils.isNotBlank(oruseend)) {
                //分端 1:教师  2:学生  3:家长  15：报表
                String useend = null;
                if (oruseend.equals("15")) {
                    useend = "15";
                } else {
                    useend = oruseend.substring(0, 1);
                }
                collector.emit(new Values(userid, useend, userip, createtime_key));
            }else{
                //异常数据处理
                WARNDATELOG.warn(CacltorPortBolt.class.getCanonicalName() + "|useend is null:" + oruseend);
            }
            //标记接收完成
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.fail(tuple);
            ERRORDATALOG.error(FormatPortBolt.class.getCanonicalName() + "|" + e.getMessage() + ":" + message);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("userid", "useend", "userip", "createtime_key"));
    }
}
