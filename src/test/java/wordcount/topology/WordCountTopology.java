package wordcount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import wordcount.bolt.CountBolt;
import wordcount.bolt.SplitBolt;
import wordcount.spout.WordsSpout;


/**
 * Created by lmz on 2017/7/25.
 */
public class WordCountTopology {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordspout", new WordsSpout(),1);
        builder.setBolt("splitbolt",new SplitBolt(),3).shuffleGrouping("wordspout");
        builder.setBolt("countbolt",new CountBolt(),3).fieldsGrouping("splitbolt",new Fields("word"));

        Config config = new Config();
        config.setDebug(false);
        config.setMaxTaskParallelism(10);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount",config,builder.createTopology());
    }

}
