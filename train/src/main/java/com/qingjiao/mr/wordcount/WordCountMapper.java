package com.qingjiao.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper:
input: 行偏移量，行数据
output:<word,1>
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    // 初始化key和value
    Text k=new Text();
    IntWritable v=new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String[] words = value.toString().split(" ");

        // 将每个单词组合<word,1>
        for (String word : words) {
            k.set(word);

            // 写出
            context.write(k,v);
        }
    }
}
