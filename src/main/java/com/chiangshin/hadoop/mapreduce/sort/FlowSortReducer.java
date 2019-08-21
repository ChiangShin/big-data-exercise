package com.chiangshin.hadoop.mapreduce.sort;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Author jx
 * @Date 2019/8/18 21:51
 */
public class FlowSortReducer extends Reducer<FlowSortBean, Text,Text,FlowSortBean> {
    FlowSortBean flowBean = new FlowSortBean();

    @Override
    protected void reduce(FlowSortBean bean, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Text phoneNum = values.iterator().next();

        context.write(phoneNum,bean);

    }
}
