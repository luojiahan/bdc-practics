package com.qingjiao.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
input:<word,[1,1,1,1]>
output:<word,4>
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    // 初始化value
    IntWritable v=new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 对key相同的数据聚合value
        int sum=0;
        for (IntWritable value : values) {
            sum+= value.get();
        }

        // 聚合后的结果封装到value
        v.set(sum);

        // 写出
        context.write(key,v);

    }
}
