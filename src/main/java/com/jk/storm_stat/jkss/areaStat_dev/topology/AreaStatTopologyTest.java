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
//        String topic_teacher_web = KafkaSource.KAFKA_TOPIC_TEACHER_WEB;
//        String topic_teacher_app = KafkaSource.KAFKA_TOPIC_TEACHER_APP;
//        String topic_teacher_pc = KafkaSource.KAFKA_TOPIC_TEACHER_PC;
//        String topic_student_app = KafkaSource.KAFKA_TOPIC_STUDENT_APP;
//        String topic_student_web = KafkaSource.KAFKA_TOPIC_STUDENT_WEB;
//        String topic_report = KafkaSource.KAFKA_TOPIC_REPORT;
        //group相关
        String group_quota_wx = KafkaSource.GROUP_QUOTA_WX;
//        String group_quota_st_web = KafkaSource.GROUP_QUOTA_ST_WEB;
//        String group_quota_st_app = KafkaSource.GROUP_QUOTA_ST_APP;
//        String group_quota_te_web = KafkaSource.GROUP_QUOTA_TE_WEB;
//        String group_quota_te_app = KafkaSource.GROUP_QUOTA_TE_APP;
//        String group_quota_te_pc = KafkaSource.GROUP_QUOTA_TE_PC;
//        String group_quota_re = KafkaSource.GROUP_QUOTA_RE;
        //7个spout的config
        SpoutConfig spoutConf_wx = new SpoutConfig(hosts, topic_wx, zkRoot, group_quota_wx);
//        SpoutConfig spoutConf_st_web = new SpoutConfig(hosts, topic_student_web, zkRoot, group_quota_st_web);
//        SpoutConfig spoutConf_st_app = new SpoutConfig(hosts, topic_student_app, zkRoot, group_quota_st_app);
//        SpoutConfig spoutConf_te_web = new SpoutConfig(hosts, topic_teacher_web, zkRoot, group_quota_te_web);
//        SpoutConfig spoutConf_te_app = new SpoutConfig(hosts, topic_teacher_app, zkRoot, group_quota_te_app);
//        SpoutConfig spoutConf_te_pc = new SpoutConfig(hosts, topic_teacher_pc, zkRoot, group_quota_te_pc);
//        SpoutConfig spoutConf_re = new SpoutConfig(hosts, topic_report, zkRoot, group_quota_re);
        //7个spout的scheme
        spoutConf_wx.scheme=new SchemeAsMultiScheme(new MessageScheme());
//        spoutConf_st_web.scheme=new SchemeAsMultiScheme(new MessageScheme());
//        spoutConf_st_app.scheme=new SchemeAsMultiScheme(new MessageScheme());
//        spoutConf_te_web.scheme=new SchemeAsMultiScheme(new MessageScheme());
//        spoutConf_te_app.scheme=new SchemeAsMultiScheme(new MessageScheme());
//        spoutConf_te_pc.scheme=new SchemeAsMultiScheme(new MessageScheme());
//        spoutConf_re.scheme=new SchemeAsMultiScheme(new MessageScheme());
        //7个spout的zkort
        spoutConf_wx.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
//        spoutConf_st_web.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
//        spoutConf_st_app.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
//        spoutConf_te_web.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
//        spoutConf_te_app.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
//        spoutConf_te_pc.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
//        spoutConf_re.zkPort = Integer.valueOf(KafkaSource.ZKPORT);
        //7个spout的 zkServer
        List<String> zkServers = new ArrayList<String>();
        zkServers.add(KafkaSource.ZKNODE5);
        zkServers.add(KafkaSource.ZKNODE6);
        zkServers.add(KafkaSource.ZKNODE7);

        spoutConf_wx.zkServers = zkServers;
//        spoutConf_st_web.zkServers = zkServers;
//        spoutConf_st_app.zkServers = zkServers;
//        spoutConf_te_web.zkServers = zkServers;
//        spoutConf_te_app.zkServers = zkServers;
//        spoutConf_te_pc.zkServers = zkServers;
//        spoutConf_re.zkServers = zkServers;

        topologyBuilder.setSpout("spoutConf_wx", new KafkaSpout(spoutConf_wx), 1);
//        topologyBuilder.setSpout("spoutConf_st_web", new KafkaSpout(spoutConf_st_web), 1);
//        topologyBuilder.setSpout("spoutConf_st_app", new KafkaSpout(spoutConf_st_app), 1);
//        topologyBuilder.setSpout("spoutConf_te_web", new KafkaSpout(spoutConf_te_web), 1);
//        topologyBuilder.setSpout("spoutConf_te_app", new KafkaSpout(spoutConf_te_app), 1);
//        topologyBuilder.setSpout("spoutConf_te_pc", new KafkaSpout(spoutConf_te_pc), 1);
//        topologyBuilder.setSpout("spoutConf_re", new KafkaSpout(spoutConf_re), 1);
        //数据中转bolt
        topologyBuilder.setBolt("transfer", new TransferBolt(),1)
//                .shuffleGrouping("spoutConf_st_app")
//                .shuffleGrouping("spoutConf_st_web")
                .shuffleGrouping("spoutConf_wx");
//                .shuffleGrouping("spoutConf_te_web")
//                .shuffleGrouping("spoutConf_te_app")
//                .shuffleGrouping("spoutConf_te_pc")
//                .shuffleGrouping("spoutConf_re");

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
