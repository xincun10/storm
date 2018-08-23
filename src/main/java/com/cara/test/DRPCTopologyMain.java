package com.cara.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

//DRPC拓扑
public class DRPCTopologyMain {

	public static void main(String[] args) {
		LocalDRPC drpc = new LocalDRPC();
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
		builder.addBolt(new DRPCBolts(), 2);
		
		Config conf = new Config();
		conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("drpc-adder-topology", conf, 
				builder.createLocalTopology(drpc));
		
		String result = drpc.execute("add", "1+-1");
		checkResult(result, 0);
		result = drpc.execute("add", "1+1+5+10");
		checkResult(result, 17);
		
		cluster.shutdown();
		drpc.shutdown();
	}

	private static boolean checkResult(String result, int expected) {
		// TODO Auto-generated method stub
		if(result != null && !result.equals("NULL")){
			if(Integer.parseInt(result) == expected){
				System.out.println("Add valid [result: "+result+"]");
				return true;
			}else{
				System.err.print("Invalid result ["+result+"]");
			}
		}else{
			System.err.println("There was an error running the drpc call");
		}
		return false;
	}
}
