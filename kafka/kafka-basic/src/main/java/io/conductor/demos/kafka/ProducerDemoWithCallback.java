package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");

        // set kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // for partition test
//        props.setProperty("batch.size", "400");

        // for partition test
        // RoundRobinPartition - 0,1,2,0,1,2,0,1,2... => loop partition one by one
        // StickyPartition (default) - 0,0,0,0,0,1,1,1,1,2,2,2,2... => loop partition by batch size (performance is better)
//        props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        // create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int j=0;j<10;j++) {
            for (int i=0;i<30;i++) {
                // create producer record
                ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello world " + i);

                // send data with callBack
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            // the record was successfully sent
                            log.info("Received new metadata. \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }



        // flush data - send all data and block until complete
        producer.flush();

        // close producer
        producer.close();


    }
}
