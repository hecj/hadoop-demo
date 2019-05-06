package com.hadoop.mr.friends;

import com.hadoop.mr.order.topn.OrderTopn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 * 案例：计算共同的好友
 *
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J
 *
 * 第一步
 * B -> A
 * B -> C
 *
 * 第二步 聚合
 * B > A C
 * 输出
 *
 * A-C -> B
 * A-C -> D
 * B-D -> A
 */
public class CommonFriendOne {

    public static class CommonFriendOneMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(":");
            String[] friends = words[1].split(",");
            v.set(words[0]);
            for(String friend :friends){
               k.set(friend);
               context.write(k,v);
            }
        }
    }

    public static class CommonFriendOneReduce extends Reducer<Text, Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> friends = new ArrayList<>();
            for(Text friend : values){
                friends.add(friend.toString());
            }
            Collections.sort(friends);
            for(int i=0;i<friends.size();i++){
                for(int j=i+1;j<friends.size();j++){
                    context.write(new Text(friends.get(i)+"-"+friends.get(j)),key);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendOne.class);

        job.setMapperClass(CommonFriendOneMapper.class);
        job.setReducerClass(CommonFriendOneReduce.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/hecj/workspace/idea/hadoop/hadoop-demo/data/friend"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/hecj/workspace/idea/hadoop/hadoop-demo/data/friend_out"));

        job.waitForCompletion(true);

    }

}
