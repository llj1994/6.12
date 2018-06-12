package com.yuantiao.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class stormOne {
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("randomLine", new one(),3);
		topologyBuilder.setBolt("splitToWord", new two(),3).shuffleGrouping("randomLine");
		topologyBuilder.setBolt("countWord", new three(),3).fieldsGrouping("splitToWord", new Fields("word"));
	}
	public static class one extends BaseRichSpout{

		private SpoutOutputCollector spoutOutputCollector;

		public void nextTuple() {
			// TODO Auto-generated method stub
			String[] strs = new String[]{"hello word pa pa"};
			String random = strs[new Random().nextInt(strs.length)];
			spoutOutputCollector.emit(new Values(random));
			Utils.sleep(10000);
		}

		public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
			// TODO Auto-generated method stub
			this.spoutOutputCollector = spoutOutputCollector;
		}

		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			// TODO Auto-generated method stub
			outputFieldsDeclarer.declare(new Fields("lines"));
		}
		
	}
	public static class two extends BaseRichBolt{
		private OutputCollector outputCollector;
		private String[] info;
		public void execute(Tuple tuple) {
			// TODO Auto-generated method stub
			info = tuple.getStringByField("line").split("\\s+");
			for (String word: info) {
				outputCollector.emit(new Values(word,1));
			}
		}

		public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
			// TODO Auto-generated method stub
			this.outputCollector = outputCollector;//初始化操作
		}

		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			// TODO Auto-generated method stub
			outputFieldsDeclarer.declare(new Fields("word","one"));
		}
	}
	public static class three extends BaseRichBolt{
		private Map<String, Integer> count = new HashMap<String, Integer>();
		private OutputCollector outputCollector;
		public void execute(Tuple tuple) {
			// TODO Auto-generated method stub
			String word = tuple.getStringByField("word");
			Integer num = tuple.getIntegerByField("one");
			Integer stat = count.get(word);//根据key得到value
			if(stat != null){
				num += stat;
			}
			count.put(word, num);
		}

		public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
			// TODO Auto-generated method stub
			this.outputCollector=outputCollector;
		}

		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			// TODO Auto-generated method stub
		}
				
	}
}
