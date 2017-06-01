package org.inanme;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;

import static org.inanme.Functions.toArray;
import static org.inanme.Functions.wait1;

public class KafkaProducerConsumerJoin {

    static void produce(String key) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        CompletableFuture[] collect = LongStream.iterate(0, Math::incrementExact)
                .mapToObj(i -> new ProducerRecord<>(key + "-topic", "person:id:" + i, new JsonObject().put(key, key + i).encode()))
                .map(record -> {
                    CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
                    wait1();
                    producer.send(record, (rm, th) -> {
                        if (th == null) {
                            System.err.printf("Producer topic:%s key:%s value:%s par:%d\n", key, record.key(), record.value(), record.partition());
                            completableFuture.complete(rm);
                        } else {
                            completableFuture.completeExceptionally(th);
                        }
                    });
                    return completableFuture;
                })
                .collect(toArray(CompletableFuture.class));
        CompletableFuture.allOf(collect);
    }

    static String mergein(String s1, String s2) {
        String result = new JsonObject(s1).mergeIn(new JsonObject(s2)).encode();
        System.err.printf("MERGER %s %s %s\n", s1, s2, result);
        return result;
    }

    static void merge(String... keys) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "merger");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> topic1 = builder.stream(keys[0]);
        KStream<String, String> topic2 = builder.stream(keys[1]);

        topic1.join(topic2, (s1, s2) -> mergein(s1, s2), JoinWindows.of(200000L));

        //topic1.print();
        //topic2.print();

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }

    public static void main(String... args) {
        //new Thread(() -> produce("name")).start();
        //new Thread(() -> produce("surname")).start();
        wait1();
        new Thread(() -> merge("name-topic", "surname-topic")).start();
    }

}
