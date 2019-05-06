package com.hadoop.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 计算每个单词 出现在哪些文件中,以及次数
 * 需要得到以下结果：
 * hello  a.txt-->4  b.txt-->4  c.txt-->4
 * java   c.txt-->1
 * jerry  b.txt-->1  c.txt-->1
 * ....
 * 倒排索引第一步
 */
public class IndexStepOne {

	public static class IndexStepOneMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 第一步输出 单词-文件 , 1
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			
			String[] words = value.toString().split(" ");
			for(String word : words){
				context.write(new Text(word+"-"+filename),new IntWritable(1));
			}
		}
	}
	
	public static class IndexStepOneReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable value : values){
				count += value.get();
			}
			context.write(key,new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReduce.class);
		
		job.setNumReduceTasks(3);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/index"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/index_out1"));
		
		job.waitForCompletion(true);
		
	}
}
