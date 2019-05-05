package com.hadoop.mr.page.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PageTopnReduce extends Reducer<Text, IntWritable,Text, IntWritable> {

    /**
     * 这种只使用单个 reduce task, 若多个的话 会生成多个top N数据
     */
    TreeMap<PageCount,Object> treeMap = new TreeMap<>();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count =0;
        for(IntWritable value : values){
            count += value.get();
        }

        PageCount pageCount = new PageCount();
        pageCount.setCount(count);
        pageCount.setPage(key.toString());

        treeMap.put(pageCount,null);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int topn = conf.getInt("top.n",5);

        Set<Map.Entry<PageCount, Object>> entrySet = treeMap.entrySet();
        int i=0;
        for(Map.Entry<PageCount, Object> entry: entrySet){
            context.write(new Text(entry.getKey().getPage()),new IntWritable(entry.getKey().getCount()));
            i++;
            if(i==topn){
                return;
            }
        }
    }
}
