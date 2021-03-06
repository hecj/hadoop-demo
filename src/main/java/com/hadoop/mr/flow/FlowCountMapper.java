package com.hadoop.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable, Text,Text,FlowBean为输入和输出参数
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
	
	/**
	 * 一行一行读取
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		String phone = fields[1];
		int upFlow = Integer.parseInt(fields[fields.length-3]);
		int dFlow = Integer.parseInt(fields[fields.length-2]);
		context.write(new Text(phone),new FlowBean(phone,upFlow,dFlow));
	}
}
