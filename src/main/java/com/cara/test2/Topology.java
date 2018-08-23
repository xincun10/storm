package com.cara.test2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("transaction-spout", new TransactionsSpouts());
		builder.setBolt("random-failure-bolt", new RandomFailureBolt())
			.shuffleGrouping("transaction-spout");
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setDebug(true);
		
		cluster.submitTopology("transactions-test", conf, builder.createTopology());
		while(true)
		{
			//wait for a failure
			Thread.sleep(1000);
		}
	}
}
