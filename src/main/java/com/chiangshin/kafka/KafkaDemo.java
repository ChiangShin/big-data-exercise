package com.chiangshin.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

/**
 * @Author jx
 * @Date 2019/10/10 22:26
 */
public class KafkaDemo {
    private final static String TOPIC = "first_topic";
    private final static String GROUP_ID = "chiangshin_1";
    public final static String BOOTSTRAP = "hadoop100:9092,hadoop101:9092,hadoop102:9092";
    //public final static String BOOTSTRAP = "172.18.102.111:9092,172.18.102.112:9092,172.18.102.113:9092";

    private Producer<String,String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // 2 构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.chiangshin.kafka.KafkaInterceptorDemo");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    private Consumer createConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("group.id", GROUP_ID);
        props.put("auto.offset.reset","earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        return consumer;
    }


    @Test
    public void product1(){
        Producer<String, String> producer = createProducer();
        for (int i = 0; i < 100000; i++) {
            String msg = "I-love-you-so-much_"+i;
            System.out.println("product   ###   "+msg);
            producer.send(new ProducerRecord<String,String>(TOPIC, Integer.toString(i), msg));
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }



    @Test
    public void consumer1(){
        new Thread(()->product1()).start();
        Consumer consumer = createConsumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> msg : records) {
                System.out.println("consumer   ###   topic:"+ msg.topic()+"  key:"+msg.key()+" value:"+msg.value());
            }
        }
    }


}
