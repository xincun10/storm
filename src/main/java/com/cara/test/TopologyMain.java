package com.cara.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		//定义拓扑
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setSpout("signals-spout", new SignalsSpout());
		
//		builder.setBolt("word-normalizer", new WordNormalizerBolt())
//			.shuffleGrouping("word-reader");
		builder.setBolt("word-normalizer", new WordNormalizerBolt())
			.customGrouping("word-reader", new ModuleGrouping());
		builder.setBolt("word-counter", new WordCounterBolt(), 2)
			.fieldsGrouping("word-normalizer", new Fields("word"))
			.allGrouping("signals-spout", "signals");
		//配置
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(true);
		//运行拓扑
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
//		StormSubmitter.submitTopology("Count-Word-Topology", conf, builder.createTopology());
	}
}
