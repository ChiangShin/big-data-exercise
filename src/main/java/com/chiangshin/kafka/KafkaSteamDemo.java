package com.chiangshin.kafka;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 *  KafkaSteam
 * @Author jx
 * @Date 2019/10/12 0:08
 */
public class KafkaSteamDemo {

    public static void main(String[] args) {
        String from = "first_topic";
        String to = "second_topic";
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaDemo.BOOTSTRAP);

        StreamsConfig config = new StreamsConfig(properties);

        Topology builder = new Topology();
        builder.addSource("SOURCE",from)
                .addProcessor("PROCESS", () -> new LogProcessor(),"SOURCE")
                .addSink("SINK",to,"PROCESS");

        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.start();
    }

}
