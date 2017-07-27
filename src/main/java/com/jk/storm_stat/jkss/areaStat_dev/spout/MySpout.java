package com.jk.storm_stat.jkss.areaStat_dev.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by lmz on 2017/7/18.
 */
public class MySpout extends BaseRichSpout {
    private TopologyContext context;
    private SpoutOutputCollector collector;
    BufferedReader br = null;
    ArrayList<String> strs = new ArrayList<>();
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector SpoutOutputCollector) {
        this.collector = SpoutOutputCollector;
        FileReader reader = null;
        String str = null;
        try {
            reader = new FileReader("c://log_temp.txt");
            br = new BufferedReader(reader);
            while((str = br.readLine()) != null) {
                System.out.println(str);
                strs.add(str);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1);
        for (String line:strs) {
            collector.emit(new Values(line));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
