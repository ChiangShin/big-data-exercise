package com.chiangshin.hadoop.mapreduce.order;

import java.io.IOException;
import javax.jdo.annotations.Order;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Author jx
 * @Date 2019/8/20 21:10
 */
public class OrderMapper extends Mapper<LongWritable,Text, OrderBean, NullWritable> {
    OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String line = value.toString();

        // 分割数据
        String[] fields = line.split("\t");

        // 塞入bean中
        String orderId = fields[0];
        double price = Double.parseDouble(fields[2]);
        orderBean.setOrderId(orderId);
        orderBean.setPrice(price);
        // 写出
        context.write(orderBean,NullWritable.get());
    }
}
