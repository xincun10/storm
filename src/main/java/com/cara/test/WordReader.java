package com.cara.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordReader implements IRichSpout {
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;
	
	public boolean isDistributed()
	{
		return false;
	}

	//创建一个文件并维持一个collector对象
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.context = context;
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
		}
		this.collector = collector;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	//分发文件中的文本行
	@Override
	public void nextTuple() {
		// 这个方法会不断的被调用，直到整个文件都读完了，等待并返回
		if(completed)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		String str;
		//创建reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			//读取所有的文本行
			while((str=reader.readLine())!=null)
			{
				//按行发布一个新值
				this.collector.emit(new Values(str), str);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("OK:"+msgId);
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("FAIL:"+msgId);
	}

	//声明输入域"word"
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
