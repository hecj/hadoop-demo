package com.hadoop.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 如果要在hadoop集群的某台机器上启动这个job提交客户端的话
 * conf里面就不需要指定fs.defaultFs mapreduce.framework.name
 * 因为在集群集群上用hadoop jar xx.jar com.hadoop.demo.JobSubmitter2 命令启动客户端main方法时
 * hadoop jar这个命令会将所有在机器上的hadoop安装目录中的jar包和配置文件加入到运行时classpath中
 * 要么我们在客户端main方法中的new configuration()语句就会加载classpath中的配置文件,自然就有了
 * fs.defaultFS和mapreduce.framework.name和yarn.resourcemanager.hostname这些参数配置
 */
public class JobSubmitter2 {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 1、封装参数：jar包所在的位置
		job.setJarByClass(JobSubmitter2.class);
		
		// 2、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		// 3、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path output = new Path("/wordcount/output2");
		FileSystem fs = FileSystem.get(new URI("hdfs://ubuntu1:9000"),conf,"hecj");
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		
		// 4、封装参数：本次job要处理的输入数据集所在路径、最终结果的输出路径
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, output);  // 注意：输出路径必须不存在
		
		
		// 5、封装参数：想要启动的reduce task的数量
		job.setNumReduceTasks(2);
		
		// 6、提交job给yarn
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}

}
