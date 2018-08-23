package com.cara.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WordCounterBolt implements IRichBolt {
	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;

	//初始化
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	//为每个单词计数
	@Override
	public void execute(Tuple input) {
		String str = null;
		try {
			//按照域接收数据
			str = input.getStringByField("word");
		} catch (IllegalArgumentException e) {
			//do nothing
		}
		if(str != null)
		{
			//可以接收到数据，计数功能
			//计数
			if(!counters.containsKey(str))
			{
				counters.put(str, 1);
			}
			else
			{
				Integer c = counters.get(str) + 1;
				counters.put(str, c);
			}
		} else {
			if(input.getSourceStreamId().equals("signals")) {
				//数据流的名字为signals，清除计数器
				str = input.getStringByField("action");
				if("refreshCache".equals(str)) {
					counters.clear();
				}
			}
		}
		
		//对元组应答
		collector.ack(input);
	}

	//spout结束时（集群关闭的时候），显示单词数量
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		System.out.println("--word-" + name + "-" + id + "--");
		for(Map.Entry<String, Integer> entry : counters.entrySet())
		{
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
