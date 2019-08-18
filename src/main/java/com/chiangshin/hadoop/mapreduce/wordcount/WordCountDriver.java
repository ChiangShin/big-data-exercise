package com.chiangshin.hadoop.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 主驱动程序
 * @Author jx
 * @Date 2019/8/18 16:07
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job对象
        Configuration config = new Configuration();
        config.set("fs.defaultFS","hdfs://hadoop100:8020");

        Job job = Job.getInstance(config);

        // 设置jar位置
        job.setJarByClass(WordCountDriver.class);

        // 设置map和reduce的class类型
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        // 设置输出mapper的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置最终数据输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入数据和输出数据的路径
        FileInputFormat.setInputPaths(job, "/user/atguigu/jj.txt");
        FileOutputFormat.setOutputPath(job, new Path("/rrr/rrr"));

        // 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }

}
