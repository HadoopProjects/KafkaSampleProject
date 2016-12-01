package com.rahul.kafka.basic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by hadoop on 29/11/16.
 */
public class MyKafkaConsumerAssign {

    public static void main(String[] str) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(properties);
        ArrayList<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
        TopicPartition partition1 = new TopicPartition("FirstTopic", 0);
        TopicPartition partition2 = new TopicPartition("SecondTopic", 2);
        topicPartitions.add(partition1);
        topicPartitions.add(partition2);
        kafkaConsumer.assign(topicPartitions);

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
