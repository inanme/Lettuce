package org.inanme;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

import static org.inanme.Functions.mysleep;

public class KafkaProducerConsumer {

    static final String TOPIC = "intstream";

    static final String SERVERS = "localhost:9093,localhost:9094";

    static final ConcurrentHashMap<String, String> MAP = new ConcurrentHashMap<>();

    static class MyProducer {
        static void produce() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            Producer<String, String> producer = new KafkaProducer<>(props);
            long start = Instant.now().toEpochMilli();
            System.err.println("Producer start " + start);
            LongStream.iterate(start, Math::incrementExact)
                    .mapToObj(i -> new ProducerRecord<>(TOPIC, "KEY-" + i, "VALUE-" + i))
                    .forEach(record -> {
                        CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
                        mysleep(1500L);
                        producer.send(record, (rm, th) -> {
                            if (th == null) {
                                System.err.println("producer Ok : " + record.key());
                                completableFuture.complete(rm);
                            } else {
                                completableFuture.completeExceptionally(th);
                            }
                        });
                    });
        }
    }

    static class MyConsumer {
        static void consume(String name) {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s-%s-%s", TOPIC, "consumer", name));
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
            props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("%s-%s", TOPIC, "consumer"));

            //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KStreamBuilder builder = new KStreamBuilder();

            builder.<String, String>stream(TOPIC).foreach((k, v) -> {
                String s = MAP.putIfAbsent(k, v);
                if (s == null) {
                    System.err.printf("%s have this (%s, %s)\n", name, k, v);
                } else {
                    System.err.printf("%s >>>> this (%s, %s)\n", name, k, v);
                }
            });

            new KafkaStreams(builder, props).start();
        }
    }

    public static void main(String... args) {
        new Thread(MyProducer::produce).start();
        new Thread(() -> MyConsumer.consume("consumer1")).start();
    }

}
