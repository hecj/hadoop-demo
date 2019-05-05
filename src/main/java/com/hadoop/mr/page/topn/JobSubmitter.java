package com.hadoop.mr.page.topn;

import com.hadoop.mr.flow.FlowBean;
import com.hadoop.mr.flow.FlowCountMapper;
import com.hadoop.mr.flow.FlowCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

//		conf.setInt("top.n",5);

		/**
		 * 通过加载classpath下的*-site.xml文件解析参数
		 */
		conf.addResource("xx-oo.xml");

		/**
		 * 通过代码设置参数
		 */
		//conf.setInt("top.n", 3);
		//conf.setInt("top.n", Integer.parseInt(args[0]));

		/**
		 * 通过属性配置文件获取参数
		 */
		/*Properties props = new Properties();
		props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
		conf.setInt("top.n", Integer.parseInt(props.getProperty("top.n")));*/

		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitter.class);
		job.setMapperClass(PageTopnMapper.class);
		job.setReducerClass(PageTopnReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job,new Path("/hadoop/topn_in"));
		FileOutputFormat.setOutputPath(job,new Path("/hadoop/topn_out"));

		// topN 只用一个reduce task,否则会生成多个topN数据
		job.setNumReduceTasks(1);
		
		job.waitForCompletion(true);
		
	}
}
