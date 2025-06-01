package com.advancedbackend.module_two.consumer;

import com.advancedbackend.module_two.model.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
    private static final String TOPIC = "user-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "user-consumer-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.reader", "true");

        Consumer<String, User> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Starting consumer... Press Ctrl+C to stop");

        try {
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, User> record : records) {
                    User user = record.value();

                    System.out.println("\n=== Received Message ===");
                    System.out.printf("Key: %s%n", record.key());
                    System.out.printf("Topic: %s, Partition: %d, Offset: %d%n",
                            record.topic(), record.partition(), record.offset());
                    System.out.printf("User ID: %d%n", user.getId());
                    System.out.printf("Name: %s%n", user.getName());
                    System.out.printf("Email: %s%n", user.getEmail());

//                     Check for V2 fields (schema evolution)
                    if (user.hasField("age") && user.getAge() != null) {
                        System.out.printf("Age: %d%n", user.getAge());
                    } else {
                        System.out.println("Age: Not provided (V1 schema)");
                    }

                    if (user.hasField("country")) {
                        System.out.printf("Country: %s%n", user.getCountry());
                    } else {
                        System.out.println("Country: Default value (V1 schema)");
                    }

                    System.out.println("========================\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
