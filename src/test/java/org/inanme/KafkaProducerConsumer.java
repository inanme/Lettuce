package org.inanme;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.LongStream;

public class KafkaProducerConsumer {
    public static class MyProducer {
        public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            Producer<String, String> producer = new KafkaProducer<>(props);
            long start = Instant.now().toEpochMilli();
            System.err.println("Producer start " + start);
            CompletableFuture[] collect = LongStream.iterate(start, Math::incrementExact)
                    .mapToObj(i -> new ProducerRecord<>("intstream-topic", "KEY-" + i, "VALUE-" + i))
                    .map(record -> {
                        CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
                        wait1();
                        producer.send(record, (rm, th) -> {
                            if (th == null) {
                                System.err.println("producer Ok : " + record.key());
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
    }

    public static class MyConsumer {
        public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "intstream-topic-consumer");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
            props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KStreamBuilder builder = new KStreamBuilder();

            builder.stream("intstream-topic").print();

            new KafkaStreams(builder, props).start();

            TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Collector<T, ArrayList<T>, T[]> toArray(Class<T> clazz) {
        return Collector.of(ArrayList::new, List::add, (left, right) -> {
            left.addAll(right);
            return left;
        }, list -> list.toArray((T[]) Array.newInstance(clazz, list.size())));
    }

    public static void wait1() {
        try {
            TimeUnit.SECONDS.sleep(1l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
