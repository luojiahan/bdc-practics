package com.qingjiao.mr.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderCompareDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderCompareDriver.class);

        job.setMapperClass(OrderCompareMapper.class);
        job.setReducerClass(OrderCompareReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义分区类
        job.setPartitionerClass(OrderPartitioner.class);

        // 设置自定义分组类
        job.setGroupingComparatorClass(OrderGroup.class);

        // 设置启动的reduce数量
        job.setNumReduceTasks(1);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        if (fs.exists(output)) {
            fs.delete(output,true);
        }

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        System.exit(job.waitForCompletion(true)?0:-1);
    }
}
