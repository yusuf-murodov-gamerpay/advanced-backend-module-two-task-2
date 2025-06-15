#!/bin/bash

echo "Starting Kafka and Schema Registry..."
docker-compose up -d

echo "Waiting for services to start..."
sleep 30

echo "Creating topic..."
docker exec -it $(docker-compose ps -q kafka) kafka-topics --create \
  --topic user-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Listing topics..."
docker exec -it $(docker-compose ps -q kafka) kafka-topics --list \
  --bootstrap-server localhost:9092

echo "Setup complete!"
echo "To run the demo:"
echo "1. mvn clean compile"
echo "2. mvn exec:java -Dexec.mainClass='com.example.SimpleKafkaConsumer' (in one terminal)"
echo "3. mvn exec:java -Dexec.mainClass='com.example.SimpleKafkaProducer' (in another terminal)"
echo "4. mvn exec:java -Dexec.mainClass='com.example.SchemaRegistryInspector' (to inspect schemas)"