package io.conductor.demos.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.Properties;

public class WikimediaChangesProducer {
    private static final String bootstrapServers = "localhost:9092";
    private static final String topic = "wikimedia.recentchange";
    private static final String url = "https://stream.wikimedia.org/v2/stream/recentchange";



    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());
        // set high throughput producer config
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Flux<String> stream = WebClient.create(url).get().retrieve().bodyToFlux(String.class);

        stream.subscribe(
                data -> {
                    System.out.println(data);
                    producer.send(
                            new ProducerRecord<>(topic, data)
                    );
                },
                error -> System.out.println("Error receiving data: " + error),
                () -> System.out.println("Completed!!!")
        );

        // produce for 10 seconds
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
