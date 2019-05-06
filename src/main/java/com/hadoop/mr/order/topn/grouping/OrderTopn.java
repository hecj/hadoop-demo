package com.hadoop.mr.order.topn.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 自定义分组和分区
 
 *
 案例数据:
 order001,u001,小米6,1999.9,2
 order001,u001,雀巢咖啡,99.0,2
 order001,u001,安慕希,250.0,2
 order001,u001,经典红双喜,200.0,4
 order001,u001,防水电脑包,400.0,2
 order002,u002,小米手环,199.0,3
 order002,u002,榴莲,15.0,10
 order002,u002,苹果,4.5,20
 order002,u002,肥皂,10.0,40
 order003,u001,小米6,1999.9,2
 order003,u001,雀巢咖啡,99.0,2
 order003,u001,安慕希,250.0,2
 order003,u001,经典红双喜,200.0,4
 order003,u001,防水电脑包,400.0,2
 
 */
public class OrderTopn {


	public static class OrderTopnMapper extends Mapper<LongWritable, Text,OrderBean,NullWritable>{
		/**
		 * 避免大量生成对象
		 */
		OrderBean orderBean = new OrderBean();
		NullWritable nullWritable = NullWritable.get();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(",");
			orderBean.setOrderId(words[0]);
			orderBean.setUserId(words[1]);
			orderBean.setPdtName(words[2]);
			orderBean.setPrice(Float.parseFloat(words[3]));
			orderBean.setNumber(Integer.parseInt(words[4]));
			orderBean.setAmounttFee();
			// 这里会序列化成二进制,所以write后就不是一个对象了
			context.write(orderBean,nullWritable);
		}
	}
	
	public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable,OrderBean, NullWritable> {
		
		/**
		 * 虽然reduce方法中的参数key只有一个，但是只要迭代器迭代一次，key中的值就会变
		 */
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			int topn = context.getConfiguration().getInt("order.top.n",3);
			int i = 0;
			for(NullWritable v : values){
				context.write(key,v);
				i++;
				if(i==topn){
					return;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.setInt("order.top.n",3);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(OrderTopn.class);
		
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		job.setPartitionerClass(OrderIdPartitioner.class);
		job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/order"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/order_out2"));
		
		job.waitForCompletion(true);
		
	}
	
}
