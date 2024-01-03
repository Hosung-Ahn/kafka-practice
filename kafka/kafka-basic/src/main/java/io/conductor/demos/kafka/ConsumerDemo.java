package io.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    private static final String GROUP_ID = "my-java-application";
    private static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        // set kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // create consumer configs
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", GROUP_ID);
        /*
        earliest - read from beginning of topic (equal to --from-beginning in CLI)
        latest - read from new messages onwards
        none - throw error if no offset is being saved
         */
        props.setProperty("auto.offset.reset", "earliest"); // earliest, latest, none

        // create kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(TOPIC)); // can subscribe to multiple topics

        while (true) {
            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));// if no data, wait 1000 ms

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + " | Value: " + record.value());
                log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
            }
        }

    }
}
