package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");

        // set kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i=0;i<10;i++) {

            String topic = "demo_java";
            String key = "id_" + i % 3;
            String value = "hello world " + i;

            // create producer record
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, key, value);


            // send data with callBack
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // the record was successfully sent
                        log.info("Key: " + key + " | Partition: " + metadata.partition());
                    } else {
                        log.error("Error while producing", exception);
                    }
                }
            });
        }



        // flush data - send all data and block until complete
        producer.flush();

        // close producer
        producer.close();


    }
}
