package com.rahul.kafka.consumergroups;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by hadoop on 29/11/16.
 */
public class ConsumerFour {

    public static void main(String[] str) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "ConsGroup");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("CGTopic"));

        try{
            while(true){
                ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
                for(ConsumerRecord record: consumerRecords){
                    System.out.println(record.topic()+", "+record.partition()+", "+record.offset()+", "+record.key()+", "+record.value());
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally {
            kafkaConsumer.close();
        }
    }
}
