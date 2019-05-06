package com.hadoop.mr.order.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区规则
 */
public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {
	
	@Override
	public int getPartition(OrderBean key, NullWritable nullWritable, int numPartitions) {
		return (key.getOrderId().hashCode()&Integer.MAX_VALUE) % numPartitions;
	}
}
