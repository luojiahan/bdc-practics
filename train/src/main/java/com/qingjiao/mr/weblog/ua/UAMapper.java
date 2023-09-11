package com.qingjiao.mr.weblog.ua;

import com.qingjiao.mr.weblog.WebLogBean;
import com.qingjiao.mr.weblog.WebLogUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper:
input:一行日志
==> 过滤非法数据，只统计合法的日志
output:<访问页面url,1>
 */
public class UAMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    // 初始化key和value
    Text k=new Text();
    IntWritable v=new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        WebLogBean webLogBean = WebLogUtils.filterLog(value.toString());
        // 判断日志是否合法
        if (webLogBean.isFlag()) {
            // 将useragent封装为key
            k.set(webLogBean.getUserAgent());

            // 写出
            context.write(k,v);
        }
    }
}
