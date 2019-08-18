package com.chiangshin.hadoop.mapreduce.flow;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Flow;

/**
 * @Author jx
 * @Date 2019/8/18 21:51
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long upFlow = 0l;
        long downFlow = 0l;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        flowBean.set(upFlow,downFlow);
        context.write(key,flowBean);

    }
}
