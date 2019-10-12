package com.chiangshin.kafka;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @Author jx
 * @Date 2019/10/12 0:38
 */
public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] key, byte[] val) {
        String input = new String(val);
        if (input.endsWith("0")){
            input.replace("0","zero");
        }else if(input.endsWith("1")){
            input.replace("1","one");
        }
    }

    @Override
    public void close() {

    }
}
