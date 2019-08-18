package com.chiangshin.hadoop.mapreduce.flow;

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
public class FlowDrive {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 获取jar 的存储路径
        job.setJarByClass(FlowDrive.class);

        // 关联map reduce 的class类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 设置map阶段的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置最后输出数据的key和value类型

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输入数据的路径和输出数据的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
