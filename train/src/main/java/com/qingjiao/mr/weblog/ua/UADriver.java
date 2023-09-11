package com.qingjiao.mr.weblog.ua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UADriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 创建Configuration对象
        Configuration conf = new Configuration();

        // 获取文件对象
        FileSystem fs = FileSystem.get(conf);

        // 创建Job对象
        Job job = Job.getInstance(conf);

        // 设置jar路径
        job.setJarByClass(UADriver.class);

        // 设置mapper和reducer类
        job.setMapperClass(UAMapper.class);
        job.setReducerClass(UAReducer.class);

        // 设置map输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出的kv的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 主函数第一个参数作为输入
        Path input = new Path(args[0]);
        // 主函数第二个参数作为输出
        Path output = new Path(args[1]);

        // 判读输出目录是否存在
        if (fs.exists(output)) {
            // 存在，删除
            fs.delete(output,true);
        }

        // 设置输入和输出目录
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        // 提交job
        System.exit(job.waitForCompletion(true)?0:-1);
    }
}
