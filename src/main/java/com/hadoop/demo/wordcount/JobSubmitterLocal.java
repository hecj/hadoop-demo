package com.hadoop.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 在本地运行mapreduce
 */
public class JobSubmitterLocal {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","file:///");
		conf.set("mapreduce.framework.name","local");
		Job job = Job.getInstance(conf);
		
		// 1、封装参数：jar包所在的位置
		job.setJarByClass(JobSubmitterLocal.class);
		
		// 2、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		// 3、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path output = new Path("/Users/hecj/Desktop/wordcount_output");
		FileInputFormat.setInputPaths(job, new Path("/Users/hecj/Desktop/wordcount"));
		FileOutputFormat.setOutputPath(job, output);
		job.setNumReduceTasks(2);
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
	}
}
