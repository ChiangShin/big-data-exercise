package com.chiangshin.hadoop.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * 订单
 * @Author jx
 * @Date 2019/8/20 20:59
 */
public class OrderBean implements WritableComparable<OrderBean>  {
    private String orderId;
    private Double price;

    public OrderBean(){}

    public OrderBean(String orderId,Double price){
        super();
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public int compareTo(OrderBean o) {
        int num = this.orderId.compareTo(o.getOrderId());
        if(num == 0){
            num = this.price > o.getPrice() ? -1 : 1;
        }
        return num;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }



    public String toString(){
        return orderId + "\t" + price;
    }
}
