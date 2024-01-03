package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");

        // set kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello world");

        // send data
        producer.send(record);

        // flush data - send all data and block until complete
        producer.flush();

        // close producer
        producer.close();


    }
}
