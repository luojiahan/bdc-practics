package com.qingjiao.mr.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    // 定义要封装的字段：订单id、商品id、消费金额
    private String orderID;
    private String productID;
    private Double cost;

    // 构造方法：空参构造用来反射调用
    public OrderBean() {
    }

    public OrderBean(String orderID, String productID, Double cost) {
        this.orderID = orderID;
        this.productID = productID;
        this.cost = cost;
    }

    // 先按照订单进行排序，订单相同的按照消费金额降序
    @Override
    public int compareTo(OrderBean o) {
        int compareOrderID = this.orderID.compareTo(o.getOrderID());

        // 如果订单id相同再比较消费金额
        if (0==compareOrderID) {
            return this.cost>o.getCost()?-1:1;

        } else {
            return compareOrderID;
        }
    }

    // 序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderID);
        dataOutput.writeUTF(productID);
        dataOutput.writeDouble(cost);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderID= dataInput.readUTF();
        this.productID=dataInput.readUTF();
        this.cost= dataInput.readDouble();
    }

    // setter和getter方法
    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public Double getCost() {
        return cost;
    }

    public void setCost(Double cost) {
        this.cost = cost;
    }

    // toString
    @Override
    public String toString() {
        return "OrderBean{" +
                "orderID='" + orderID + '\'' +
                ", productID='" + productID + '\'' +
                ", cost=" + cost +
                '}';
    }
}
