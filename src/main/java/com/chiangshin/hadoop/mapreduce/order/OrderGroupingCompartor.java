package com.chiangshin.hadoop.mapreduce.order;

import javax.jdo.annotations.Order;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author jx
 * @Date 2019/8/20 21:33
 */
public class OrderGroupingCompartor extends WritableComparator {
    public OrderGroupingCompartor(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        return aBean.getOrderId().compareTo(bBean.getOrderId());

    }
}
