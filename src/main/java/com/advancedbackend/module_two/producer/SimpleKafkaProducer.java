package com.advancedbackend.module_two.producer;

import com.advancedbackend.module_two.model.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaProducer {
    private static final String TOPIC = "user-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        Producer<String, User> producer = new KafkaProducer<>(props);

        try {
            // Send messages with different versions
            sendUserV1(producer);
            Thread.sleep(2000); // Wait a bit between sends
            sendUserV2(producer);

            System.out.println("Messages sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void sendUserV1(Producer<String, User> producer) throws ExecutionException, InterruptedException {
        System.out.println("Sending User V1 message...");

        User user = User.newBuilder()
                .setId(1L)
                .setName("John Doe")
                .setEmail("john.doe@example.com")
                .build();

        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC, "user-1", user);

        producer.send(record, (metadata1, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.printf("Sent User V1 - Topic: %s, Partition: %d, Offset: %d%n",
                        metadata1.topic(), metadata1.partition(), metadata1.offset());
            }
        }).get();
    }

    private static void sendUserV2(Producer<String, User> producer) throws ExecutionException, InterruptedException {
        System.out.println("Sending User V2 message (with new fields)...");

        // Using User V2 schema with additional fields
        User user = User.newBuilder()
                .setId(2L)
                .setName("Jane Smith")
                .setEmail("jane.smith@example.com")
                .setAge(30)
                .setCountry("USA")
                .build();

        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC, "user-2", user);

        RecordMetadata metadata = producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.printf("Sent User V2 - Topic: %s, Partition: %d, Offset: %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            }
        }).get();
    }
}