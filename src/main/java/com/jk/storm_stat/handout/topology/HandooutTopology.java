package com.jk.storm_stat.handout.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.jk.storm_stat.handout.bolt.HandoutBolt;
import com.jk.storm_stat.handout.bolt.LowerBolt;
import com.jk.storm_stat.handout.bolt.UpperBolt;
import com.jk.storm_stat.handout.spout.MySpout;


/**
 * Created by lmz on 2017/7/24.
 * 一个bolt 通过不同的streamId 发给不同的bolt   主类：HandoutBolt()
 */
public class HandooutTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spoutConf_all", new MySpout(), 1);

        topologyBuilder.setBolt("HandoutBolt", new HandoutBolt(), 4).shuffleGrouping("spoutConf_all");
        //接受的bolt处。设置上steam id。
        topologyBuilder.setBolt("GetA", new LowerBolt(), 4)
                .shuffleGrouping("HandoutBolt","lower");
        topologyBuilder.setBolt("GetZ", new UpperBolt(), 4)
                .shuffleGrouping("HandoutBolt","upper");

        Config conf = new Config();

        conf.setDebug(false);

        if (args != null && args.length > 0) {

            try {
                conf.setNumWorkers(2);
                StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {

            conf.setMaxTaskParallelism(20);
            LocalCluster cluster = new LocalCluster();

            //自定义topology名称
            cluster.submitTopology("PvUvStat", conf, topologyBuilder.createTopology());
            // 本地10秒后关闭
            // Thread.sleep(10000);
            // cluster.shutdown();
        }
    }
}
