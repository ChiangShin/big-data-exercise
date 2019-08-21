package com.chiangshin.hadoop.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  对数据进行分区
 * @Author jx
 * @Date 2019/8/19 19:13
 */
public class FlowPartitioner extends Partitioner<Text,FlowBean> {

    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneNum = text.toString().substring(0, 3);

        if ("135".equals(phoneNum)){
            return 0;
        }else if("136".equals(phoneNum)){
            return 1;
        }else if("137".equals(phoneNum)){
            return 2;
        }else if("138".equals(phoneNum)){
            return 3;
        }else{
            return 4;
        }
    }
}
