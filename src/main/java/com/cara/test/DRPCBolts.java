package com.cara.test;

import java.security.InvalidParameterException;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class DRPCBolts extends BaseBasicBolt {
	private static final Object NULL = "NULL";
	private OutputCollector collector;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//parse the add expression
		String[] numbers = input.getString(1).split("\\+");
		Integer added = 0;
		try {
			if(numbers.length < 2)
			{
				throw new InvalidParameterException("Should be at least 2 numbers");
			}
			for(String num : numbers)
			{
				added += Integer.parseInt(num);
			}
		}catch(Exception e) {
			collector.emit(new Values(input.getValue(0), NULL));
		}
		//因为这是拓扑中惟一的 bolt，它必须发布 RPC ID 和结果。execute 方法负责执行加法运算。
		collector.emit(new Values(input.getValue(0), added));
	}

	//声明输出
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "result"));
	}

}
