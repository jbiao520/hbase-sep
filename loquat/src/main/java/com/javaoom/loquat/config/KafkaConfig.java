package com.javaoom.loquat.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;

public class KafkaConfig {
    private static Properties producerProp = new Properties();
    private static Properties consumerProp = new Properties();

    private KafkaConfig() {
    }

    static {

        Properties properties = null;
        try {
            properties = PropertiesLoaderUtils.loadAllProperties("kafka.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String env = properties.getProperty("env");
        try {
            if(env.equalsIgnoreCase("company")){
                producerProp.put("bootstrap.servers", "localhost:9092");
                consumerProp.put("bootstrap.servers", "localhost:9092");
            }else{
                producerProp.put("bootstrap.servers", properties.getProperty("bootstrap.servers"));
                consumerProp.put("bootstrap.servers", properties.getProperty("bootstrap.servers"));
            }


            producerProp.put("key.serializer", properties.getProperty("key.serializer"));
            producerProp.put("value.serializer", properties.getProperty("value.serializer"));
            producerProp.put("batch.size", properties.getProperty("batch.size"));
            producerProp.put("buffer.memory", properties.getProperty("buffer.memory"));
            producerProp.put("linger.ms", properties.getProperty("linger.ms"));
            producerProp.put("acks", properties.getProperty("acks"));

            consumerProp.put("group.id", properties.getProperty("group.id"));
            consumerProp.put("enable.auto.commit", "true");
            consumerProp.put("key.deserializer", properties.getProperty("key.deserializer"));
            consumerProp.put("value.deserializer", properties.getProperty("value.deserializer"));
            consumerProp.put("auto.offset.reset", properties.getProperty("auto.offset.reset"));
            consumerProp.put("max.poll.records", properties.getProperty("max.poll.records"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static KafkaProducer createProducer(){
        return new KafkaProducer<>(producerProp);
    }

    public static KafkaConsumer createConsumer(String groupId){
        consumerProp.put("group.id",groupId);
        return new KafkaConsumer<>(consumerProp);
    }

}
