package com.jk.storm_stat.jkss.areaStat_dev.spout;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * 数据解析bolt
 */
public class TransferBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4752656887774402264L;
	private TopologyContext context;
	private OutputCollector collector;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}
	
	public void execute(Tuple tuple) {
		String message = null;
		try{
//			byte[] bytes = tuple.getBinary(0);
//	        String message = new String(bytes);
			message = (String)tuple.getValue(0);
			collector.emit(new Values(message));
			collector.ack(tuple);
		}catch(Exception e){
			collector.fail(tuple);
			System.out.println("TransferBolt message:" + message);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
}
