package com.qingjiao.mr.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        // 将订单id相同的数据放入同一个分区
        return (orderBean.getOrderID().hashCode() & Integer.MAX_VALUE)%i;
    }
}
