package com.cara.test;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordNormalizerBolt implements IRichBolt{
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	//bolt从单词文件接收到文本行，并标准化它
	//文本行会全部转化成小写，并切分他，从中得到所有单词
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for(String word:words)
		{
			word = word.trim();
			if(!word.isEmpty())
			{
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
		//对元组做出应答
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	//声明bolt的出参
	//这个bolt只会发布word域
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//发布一个名为word的域
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
