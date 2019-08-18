package com.chiangshin.hadoop.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Author jx
 * @Date 2019/8/18 15:57
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // 统计所有单词的个数
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        // 输出该单词的总次数
        context.write(key,new IntWritable(count));

    }
}
