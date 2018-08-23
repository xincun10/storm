package com.cara.test2;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TransactionsSpouts extends BaseRichSpout {
	private static final Integer MAX_FAILS = 2;
	Map<Integer, String> messages;
	Map<Integer, Integer> transactionFailureCount;
	Map<Integer, String> toSend;
	private SpoutOutputCollector collector;
	
	static Logger LOG = Logger.getLogger(TransactionsSpouts.class);

	//初始化工作
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		Random random = new Random();
		messages = new HashMap<Integer, String>();
		toSend = new HashMap<Integer, String>();
		transactionFailureCount = new HashMap<Integer, Integer>();
		//spout随机发送100个事务ID
		for(int i=0; i<100; i++)
		{
			messages.put(i, "transaction_" + random.nextInt());
			transactionFailureCount.put(i, 0);
		}
		toSend.putAll(messages);
		this.collector = collector;
	}

	//处理每个元组
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(!toSend.isEmpty()) {
			for(Map.Entry<Integer, String> transactionEntry : toSend.entrySet())
			{
				Integer transactionId = transactionEntry.getKey();
				String transactionMsg = transactionEntry.getValue();
				/*
				 * Emits a new tuple to the default output stream with the given message ID. 
				 * When Storm detects that this tuple has been fully processed, 
				 * or has failed to be fully processed, the spout will receive an ack 
				 * or fail callback respectively with the messageId as long as the messageId 
				 * was not null. 
				 */
				collector.emit(new Values(transactionMsg), transactionId);
			}
			toSend.clear();
		}
		try {
			Thread.sleep(1000);
		}catch(InterruptedException e) {}
	}

	@Override
	public void close() {
		
	}

	//每个元组处理成功的处理方式
	@Override
	public void ack(Object msgId) {
		messages.remove(msgId);
		LOG.info("Message fully proccssed [" + msgId + "]");
	}

	//元组处理失败的处理方式
	@Override
	public void fail(Object msgId) {
		//判断该ID事务是否存在
		if(!transactionFailureCount.containsKey(msgId))
		{
			throw new RuntimeException("Error, transactionId " + msgId + "not exists");
		}
		Integer transactionId = (Integer) msgId;
		//计算失败次数
		Integer failures = transactionFailureCount.get(transactionId) + 1;
		if(failures >= MAX_FAILS){
			//失败次数太多，终止拓扑运行
			throw new RuntimeException("Error, transaction id ["+transactionId+
					"] has had many errors ["+failures+"]");
		}
		transactionFailureCount.put(transactionId, failures);
		//重新加入到发送Map中
		toSend.put(transactionId, messages.get(transactionId));
		LOG.info("Re-sending message [" + msgId + "]");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("transactionMessage"));
	}

}
