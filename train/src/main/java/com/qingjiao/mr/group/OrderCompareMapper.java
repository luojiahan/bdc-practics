package com.qingjiao.mr.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderCompareMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] contents = value.toString().split("\t");

        OrderBean orderBean = new OrderBean(contents[0], contents[1], Double.parseDouble(contents[2]));

        context.write(orderBean,NullWritable.get());
    }
}
