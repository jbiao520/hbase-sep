package com.javaoom.loquat.processors;

import com.javaoom.loquat.config.KafkaConfig;
import com.javaoom.loquat.consumers.LoggingConsumer;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventKafkaSender implements EventListener {
    private static Logger logger = LoggerFactory.getLogger(EventKafkaSender.class);
    private static volatile KafkaProducer kafkaProducer = null;

    private String topic;

    public EventKafkaSender(String topic) {
        this.topic = topic;
    }

    @Override
    public void processEvents(List<SepEvent> sepEvents) {
        if (kafkaProducer == null) {
            synchronized (KafkaProducer.class){
                kafkaProducer = KafkaConfig.createProducer();
            }
        }
        sepEvents.forEach(sepEvent -> {
            ProducerRecord<String,SepEvent> record = new ProducerRecord<>(topic, sepEvent);
            kafkaProducer.send(record);
        });

    }
}
