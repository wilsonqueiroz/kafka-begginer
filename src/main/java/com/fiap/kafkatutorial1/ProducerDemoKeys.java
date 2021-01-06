package com.fiap.kafkatutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "kafka-course";
        String value = "hello kafka";
        String key = UUID.randomUUID().toString();

        //Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //Create Producer Record
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,value,key);

        //Send data
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e ==null){
                    logger.info("Received new metada. \n" + "Topic:" + recordMetadata.topic() + "\n " + "Partition" + recordMetadata.partition()+ "\n"+"Offset" + recordMetadata.offset()+ "\n" + "Time" + recordMetadata.timestamp());
                }else{
                    logger.error( "Erro while producing", e);
                }
            }
        }).get();

        producer.flush();

        producer.close();



    }
}
