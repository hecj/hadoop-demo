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
 * 倒排索引第二步
 *
 * 案例数据:
 * hello-c.txt	4
 * jack-b.txt	1
 * java-c.txt	1
 * jerry-b.txt	1
 * kitty-a.txt	1
 * rose-a.txt	1
 *
 */
public class IndexStepTwo {

	public static class IndexStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("-");
			context.write(new Text(words[0]),new Text(words[1].replaceAll("\t","-->")));
		}
	}
	
	public static class IndexStepTwoReduce extends Reducer<Text,Text,Text,Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for(Text value : values){
				sb.append(value.toString()).append("\t");
			}
			context.write(key,new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepTwo.class);
		
		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReduce.class);
		
		job.setNumReduceTasks(3);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/index_out1"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/hecj/workspace/idea/hadoop-demo/data/index_out2"));
		
		job.waitForCompletion(true);
		
	}
}
