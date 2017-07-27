package wordcount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by lmz on 2017/7/25.
 */
public class CountBolt extends BaseRichBolt{
    HashMap<String,Integer> maps = new HashMap();
    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = (String) tuple.getValueByField("word");
        if(maps.containsKey(word)){
            Integer n = maps.get(word);
            n=n+1;
            maps.put(word,n);
        }else{
            maps.put(word,1);
        }
        Set<String> keys = maps.keySet();
        for (String key:keys) {
            System.out.println(Thread.currentThread().getName()+":  单词："+key+" : "+maps.get(key));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
