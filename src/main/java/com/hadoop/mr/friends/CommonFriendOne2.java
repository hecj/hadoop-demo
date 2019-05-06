package com.hadoop.mr.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 案例：计算共同的好友个数
 *
 * B-C	A
 * B-D	A
 * B-F	A
 * B-G	A
 * B-H	A
 * B-I	A
 * B-K	A
 * B-O	C
 * C-D	F
 * C-F	A
 * C-G	A
 * C-H	A
 *
 * 直接wordcount即可实现
 *
 */
public class CommonFriendOne2 {

    public static class CommonFriendOneMapper2 extends Mapper<LongWritable, Text,Text, IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            k.set(words[0]);
            context.write(k,v);
        }
    }

    public static class CommonFriendOneReduce2 extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values){
                count += value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendOne2.class);

        job.setMapperClass(CommonFriendOneMapper2.class);
        job.setReducerClass(CommonFriendOneReduce2.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/hecj/workspace/idea/hadoop/hadoop-demo/data/friend_out"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/hecj/workspace/idea/hadoop/hadoop-demo/data/friend_out2"));

        job.waitForCompletion(true);

    }

}
