package com.chiangshin.hadoop.mapreduce.sort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author jx
 * @Date 2019/8/18 21:55
 */
public class FlowSortDrive {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置信息
        Configuration configuration = new Configuration();
        // 本地运行时，注销此代码
        //configuration.set("fs.defaultFS","hdfs://hadoop100:8020");
        Job job = Job.getInstance(configuration);

        // 获取jar 的存储路径
        job.setJarByClass(FlowSortDrive.class);

        // 关联map reduce 的class类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        // 设置map阶段的key和value类型
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);
//        // 设置分区
//        job.setPartitionerClass(FlowPartitioner.class);
//        // 设置reducenum
//        job.setNumReduceTasks(5);
        // 设置最后输出数据的key和value类型

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);

        //设置输入数据的路径和输出数据的路径
        // 设置输入数据和输出数据的路径
//        FileInputFormat.setInputPaths(job, "/phone_data.txt");
//        FileOutputFormat.setOutputPath(job, new Path("/output/phone_data"));
        FileInputFormat.setInputPaths(job, "D:\\test\\output\\phone_data\\part-r-00000");
        FileOutputFormat.setOutputPath(job, new Path("D:\\test\\output\\phone_data_sort"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
