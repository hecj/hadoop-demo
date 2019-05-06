package com.hadoop.mr.order.topn;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
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


	public static class OrderTopnMapper extends Mapper<LongWritable, Text,Text,OrderBean>{
		/**
		 * 避免大量生成对象
		 */
		OrderBean orderBean = new OrderBean();
		Text k = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(",");
			orderBean.setOrderId(words[0]);
			orderBean.setUserId(words[1]);
			orderBean.setPdtName(words[2]);
			orderBean.setPrice(Float.parseFloat(words[3]));
			orderBean.setNumber(Integer.parseInt(words[4]));
			orderBean.setAmounttFee();
			k.set(orderBean.getOrderId());
			// 这里会序列化成二进制,所以write后就不是一个对象了
			context.write(k,orderBean);
		}
	}
	
	public static class OrderTopnReducer extends Reducer<Text, OrderBean,OrderBean, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
		
			int topn = context.getConfiguration().getInt("order.top.n",3);
			
			List<OrderBean> bealist = new ArrayList<>();
			// reduce task提供的values迭代器,每次返回的都是同一对象,只是set了不同的值
			for(OrderBean orderBean : values){
				OrderBean newOrderBean = new OrderBean();
				newOrderBean.setOrderId(orderBean.getOrderId());
				newOrderBean.setNumber(orderBean.getNumber());
				newOrderBean.setPrice(orderBean.getPrice());
				newOrderBean.setPdtName(orderBean.getPdtName());
				newOrderBean.setUserId(orderBean.getUserId());
				newOrderBean.setAmounttFee();
				bealist.add(newOrderBean);
			}
			// beanlist进行倒序排序
			Collections.sort(bealist);
			
			for(int i=0;i<topn && i<bealist.size();i++){
				context.write(bealist.get(i),NullWritable.get());
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
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/order"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/order_out"));
		
		job.waitForCompletion(true);
		
	}
	
}
