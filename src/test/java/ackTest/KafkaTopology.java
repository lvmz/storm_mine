package ackTest;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.jk.storm_stat.util.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Properties;

/**
 * Created by lmz on 2017/8/2.
 */
public class KafkaTopology {
    public static void main(String[] args) throws Exception {
        // 配置Zookeeper地址
        BrokerHosts brokerHosts;
        brokerHosts = new ZkHosts("localhost:2181");
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "msgTopic1", "/topology/root1", "topicMsgTopology");
        // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();
        Properties props = new Properties();
        // 配置Kafka broker地址
        props.put("metadata.broker.list", "localhost:9092");
        // serializer.class为消息的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", props);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "msgTopic2");
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("msgKafkaSpout", new KafkaSpout(spoutConfig),2);
//        builder.setBolt("msgSentenceBolt", (IBasicBolt) new TopicMsgBolt()).shuffleGrouping("msgKafkaSpout");
//        builder.setBolt("msgKafkaBolt", new KafkaBolt<String, Integer>()).shuffleGrouping("msgSentenceBolt");
        if (args.length == 0) {
            String topologyName = "kafkaTopicTopology";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
