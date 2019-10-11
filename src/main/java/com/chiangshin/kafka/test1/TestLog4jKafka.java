package com.chiangshin.kafka.test1;

import org.apache.log4j.Logger;

/**
 * @author jx
 * @date 2019/9/26 10:42
 */
public class TestLog4jKafka {
    private static Logger logger = Logger.getLogger(TestLog4jKafka.class);

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 10000; i++) {
            logger.info("这是一个晴朗的早上"+i+"，早起的虫儿被鸟吃");
        }
    }
}
