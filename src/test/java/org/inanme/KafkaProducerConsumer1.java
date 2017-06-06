package org.inanme;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.Properties;

import static org.inanme.Functions.mysleep;

/**
 * if you provide more threads than there are partitions on the topic, some threads will never see a message
 * if you have more partitions than you have threads, some threads will receive data from multiple partitions
 * if you have multiple partitions per thread there is NO guarantee about the order you receive messages, other than that within the partition the offsets will be sequential. For example, you may receive 5 messages from partition 10 and 6 from partition 11, then 5 more from partition 10 followed by 5 more from partition 10 even if partition 11 has data available.
 * adding more processes/threads will cause Kafka to re-balance, possibly changing the assignment of a Partition to a Thread.
 */
public class KafkaProducerConsumer1 {

    private static final String TOPIC = "a-topic";
    private static final String CONSUMER_GROUP = "a-group";
    static final String SERVERS = "localhost:9093,localhost:9094";

    static class KafkaConsumerDrama implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final String name;

        KafkaConsumerDrama(String name) {
            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
            kafkaProps.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s-%s-%s", TOPIC, "consumer", name));
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            this.consumer = new KafkaConsumer<>(kafkaProps);
            this.name = name;
        }

        @Override
        public void run() {
            consume();
        }

        private void consume() {
            consumer.subscribe(Arrays.asList(TOPIC));
            while (true) {
                ConsumerRecords<String, String> msg = consumer.poll(3000L);
                for (ConsumerRecord<String, String> record : msg) {
                    System.err.printf("%s : key: %s, par: %s\n", name, record.key(), record.partition());
                }
            }
        }
    }

    static class KafkaProducerDrama implements Runnable {

        private final KafkaProducer<String, String> kafkaProducer;

        KafkaProducerDrama() {
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
            //kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "100"); //commented out on purpose
            this.kafkaProducer = new KafkaProducer<>(kafkaProps);
        }

        @Override
        public void run() {
            produce();
        }

        private void produce() {
            int i = 0;
            while (true) {
                String key = "key-" + i;
                String value = "value-" + i;
                i++;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
                kafkaProducer.send(record, (rm, th) -> {
                    if (th != null) {
                        th.printStackTrace();
                    } else {
                        System.err.printf("pro : key: %s, par: %s\n", key, rm.partition());
                    }
                });
                mysleep(2000L);
            }
        }
    }

    public static void main(String... args) {
        //bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic a-topic
        //https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
        new Thread(new KafkaConsumerDrama("cn0")).start();
        new Thread(new KafkaConsumerDrama("cn1")).start();
        new Thread(new KafkaConsumerDrama("cn2")).start();
        new Thread(new KafkaProducerDrama()).start();
    }
}
