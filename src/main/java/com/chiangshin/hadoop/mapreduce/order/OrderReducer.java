package com.chiangshin.hadoop.mapreduce.order;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Author jx
 * @Date 2019/8/20 21:23
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        context.write(key,NullWritable.get());

    }
}
