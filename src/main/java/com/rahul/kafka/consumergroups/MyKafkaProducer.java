package com.rahul.kafka.consumergroups;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by rsahukar on 11/29/2016.
 */
public class MyKafkaProducer {

    public static void main(String[] str) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);

        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord("CGTopic", "Message "+i));
        }

        kafkaProducer.close();
    }

}