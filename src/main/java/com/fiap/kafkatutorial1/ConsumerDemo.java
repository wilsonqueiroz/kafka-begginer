package com.fiap.kafkatutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        System.out.println("Hello");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId ="my-fourth-app";

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        // CONSUMER CONFIGS
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //CREATE CONSUMER

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //  SUBSCRIBE CONSUMER TO OUR TOPICS
        consumer.subscribe(Collections.singleton("first_topic"));


    }
}
