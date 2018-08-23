package com.cara.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

/*
 * -让我们修改单词计数器示例，采用单词首字母字符的整数值与任务数的余数，决定接收元组的 bolt
 */
public class ModuleGrouping implements CustomStreamGrouping, Serializable{
	int numTasks = 0;
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, 
			List<Integer> targetTasks) {
		numTasks = targetTasks.size();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>();
		if(values.size() > 0)
		{
			String str = values.get(0).toString();
			if(str.isEmpty())
			{
				boltIds.add(0);
			}
			else
			{
				boltIds.add(str.charAt(0) % numTasks);
			}
		}
		return boltIds;
	}

}
