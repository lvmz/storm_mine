package com.jk.storm_stat.jkss.areaStat_dev.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_area.CacltorAreaBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_area.FormatAreaBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_area.PersitAreaBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_port.CacltorPortBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_port.FormatPortBolt;
import com.jk.storm_stat.jkss.areaStat_dev.bolt_port.PersitPortBolt;
import com.jk.storm_stat.jkss.areaStat_dev.spout.TransferBolt;
import com.jk.storm_stat.util.KafkaSource;
import com.jk.storm_stat.util.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lmz on 2017/7/21.
 */
public class AreaStatTopologyTest {
    public static void main(String[] args) {
        String zkHost = KafkaSource.KAFKA_ZKHOSTS;
        String zkRoot = KafkaSource.KAFKA_ZKROOT;
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts(zkHost);
        //topic相关。
        String topic_wx = "testlv";//"log_family_parent_wx";// 主题
        //group相关
        String group_quota_wx = KafkaSource.GROUP_QUOTA_WX;
        //7个spout的config
        SpoutConfig spoutConf_wx = new SpoutConfig(hosts, topic_wx, zkRoot, group_quota_wx);
        //7个spout的scheme
        spoutConf_wx.scheme=new SchemeAsMultiScheme(new MessageScheme());
        //7个spout的zkort
        spoutConf_wx.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
        //7个spout的 zkServer
        List<String> zkServers = new ArrayList<String>();
        zkServers.add(KafkaSource.ZKNODE5);
        zkServers.add(KafkaSource.ZKNODE6);
        zkServers.add(KafkaSource.ZKNODE7);

        spoutConf_wx.zkServers = zkServers;

        topologyBuilder.setSpout("spoutConf_wx", new KafkaSpout(spoutConf_wx), 1);
        //数据中转bolt
        topologyBuilder.setBolt("transfer", new TransferBolt(),1)
                .shuffleGrouping("spoutConf_wx");

        topologyBuilder.setBolt("formatAreaBolt", new FormatAreaBolt(),1).shuffleGrouping("transfer");
        topologyBuilder.setBolt("cacltorAreaBolt", new CacltorAreaBolt(), 1).shuffleGrouping("formatAreaBolt");
        topologyBuilder.setBolt("persitAreaBolt", new PersitAreaBolt(), 1).shuffleGrouping("cacltorAreaBolt");

        /*
         * 按端
         */
        topologyBuilder.setBolt("formatPortBolt", new FormatPortBolt(),1).shuffleGrouping("transfer");
        topologyBuilder.setBolt("cacltorPortBolt", new CacltorPortBolt(),1).shuffleGrouping("formatPortBolt");
        topologyBuilder.setBolt("persitPortBolt", new PersitPortBolt(),1).shuffleGrouping("cacltorPortBolt");


        Config config = new Config();
        config.setDebug(false);
        String mode = KafkaSource.MODE;
        try {
            if (mode.equals("Local")) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("local-useend-area-stat", config, topologyBuilder.createTopology());
            } else if (mode.equals("Remote")) {
                config.setNumWorkers(2);
                StormSubmitter.submitTopology("useend-area-stat", config, topologyBuilder.createTopology());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
