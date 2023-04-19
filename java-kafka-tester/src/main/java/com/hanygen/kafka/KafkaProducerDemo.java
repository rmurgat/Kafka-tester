package com.hanygen.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * [origen:] https://www.conduktor.io/kafka/complete-kafka-producer-with-java
 * Code copied and executed 25/Feb/2023 at Linux/ThinkCenter Tiny computer
 */
public class KafkaProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerDemo.class);

        public static void main(String[] args) {
            log.info("I am a Kafka Producer");

            String bootstrapServers = "127.0.0.1:9092";

            // create Producer properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("rubenpipe", "hello world");

            // send data - asynchronous
            producer.send(producerRecord);

            // flush data - synchronous
            producer.flush();
            // flush and close producer
            producer.close();
    }
}