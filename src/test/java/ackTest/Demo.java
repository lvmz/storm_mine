package ackTest;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lmz on 2017/8/1.
 */
public class Demo {
    public class RandomSentenceSpout extends BaseRichSpout {
        private SpoutOutputCollector _collector;
        private Random _rand;
        private ConcurrentHashMap<UUID, Values> _pending;

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
            _rand = new Random();
            _pending = new ConcurrentHashMap<UUID, Values>();
        }

        public void nextTuple() {
            Utils.sleep(1000);
            String[] sentences = new String[] {
                    "I write php",
                    "I learning java",
                    "I want to learn swool and tfs"
            };
            String sentence = sentences[_rand.nextInt(sentences.length)];
            Values v = new Values(sentence);
            UUID msgId = UUID.randomUUID();
            this._pending.put(msgId, v);//spout对发射的tuple进行缓存
            _collector.emit(v, msgId);//发射tuple时，添加msgId

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("world"));
        }

        public void ack(Object msgId) {
            this._pending.remove(msgId);//对于成功处理的tuple从缓存队列中删除

        }

        public void fail(Object msgId) {
            this._collector.emit(this._pending.get(msgId), msgId);//当消息处理失败了，重新发射，当然也可以做其他的逻辑处理

        }
    }

    public class SplitSentence extends BaseRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" "))
                _collector.emit(tuple, new Values(word));//发射tuple时进行锚定

            _collector.ack(tuple);//对处理完的tuple进行确认

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }
}
