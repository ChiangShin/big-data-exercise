package com.chiangshin.hadoop.mapreduce.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * @Author jx
 * @Date 2019/8/18 21:27
 */
public class FlowBean implements Writable {

    /**
     * 上行流量
     */
    private long upFlow;
    /**
     * 下行流量
     */
    private long downFlow;
    /**
     * 总流量
     */
    private long sumFlow;

    public FlowBean(){
        super();
    }
    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    public void set(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();

    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    @Override
    public String toString() {
        return upFlow +"\t"+ downFlow  +"\t"+ sumFlow ;
    }

}
