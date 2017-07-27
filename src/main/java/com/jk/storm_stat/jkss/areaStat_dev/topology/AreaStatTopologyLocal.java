package com.jk.storm_stat.jkss.areaStat_dev.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.jk.storm_stat.jkss.areaStat_dev.bolt_area.CacltorAreaBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_area.FormatAreaBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_area.PersitAreaBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_port.CacltorPortBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_port.FormatPortBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_port.PersitPortBolt;
import com.jk.storm_stat.jkss.areaStat_dev.spout.MySpout;

/**
 * Created by lmz on 2017/7/18.
 */
public class AreaStatTopologyLocal {
    public static void main(String[] args)  {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spoutConf_all", new MySpout(), 1);
        //解析日志bolt
        /**
         * 按端统计拓扑
         */
        topologyBuilder.setBolt("FormatPortBolt",new FormatPortBolt(),1).shuffleGrouping("spoutConf_all");
        topologyBuilder.setBolt("CacltorPortBolt",new CacltorPortBolt(),1).shuffleGrouping("FormatPortBolt");
        topologyBuilder.setBolt("PersitPortBolt",new PersitPortBolt(),1).shuffleGrouping("CacltorPortBolt");
        /**
         * 按区域统计拓扑
         */
        topologyBuilder.setBolt("FormatAreaBolt",new FormatAreaBolt(),1).shuffleGrouping("spoutConf_all");
        topologyBuilder.setBolt("CacltorAreaBolt",new CacltorAreaBolt(),1).shuffleGrouping("FormatAreaBolt");
        topologyBuilder.setBolt("PersitAreaBolt",new PersitAreaBolt(),1).shuffleGrouping("CacltorAreaBolt");

        Config conf = new Config();

        conf.setDebug(false);

        if (args != null && args.length > 0) {

            try {
                conf.setNumWorkers(2);
                StormSubmitter.submitTopology(args[0], conf,topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {

            conf.setMaxTaskParallelism(20);

            LocalCluster cluster = new LocalCluster();

            //自定义topology名称
            cluster.submitTopology("PvUvStat", conf,topologyBuilder.createTopology());
            // 本地10秒后关闭
            // Thread.sleep(10000);
            // cluster.shutdown();
        }
    }
}
