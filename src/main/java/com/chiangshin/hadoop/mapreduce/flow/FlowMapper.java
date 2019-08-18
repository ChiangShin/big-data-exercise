package com.chiangshin.hadoop.mapreduce.flow;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Author jx
 * @Date 2019/8/18 21:34
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    FlowBean flowBean = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String line = value.toString();

        // 截取字段
        String[] arr = line.split("\t");

        // 封装bean对象及获取电话号码

        String phoneNum = arr[1];
        int length = arr.length;
        long upFlow = Long.parseLong(arr[length-3]);
        long downFlow = Long.parseLong(arr[length-2]);

        flowBean.set(upFlow,downFlow);
        k.set(phoneNum);
        // 写出去
        context.write(k,flowBean);
    }
}
