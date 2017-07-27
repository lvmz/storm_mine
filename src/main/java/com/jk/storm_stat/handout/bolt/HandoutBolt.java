package com.jk.storm_stat.handout.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by lmz on 2017/7/24.
 */
public class HandoutBolt extends BaseRichBolt{
    OutputCollector collector;
    StringBuilder upperStr = new StringBuilder();
    StringBuilder lowerStr = new StringBuilder();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String str = tuple.getString(0);
        int i = 0;
        char c = str.charAt(i);
        if(Character.isLowerCase(c)){
            collector.emit("lower", new Values(str));
        }else{
            collector.emit("upper", new Values(str));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("upper", new Fields("upper"));
        declarer.declareStream("lower", new Fields("lower"));
    }
}
