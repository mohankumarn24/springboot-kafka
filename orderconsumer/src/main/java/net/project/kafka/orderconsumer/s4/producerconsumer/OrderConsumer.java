package net.project.kafka.orderconsumer.s4.producerconsumer;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup"); // https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderTopic"));

        ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, Integer> record: records) {
            System.out.println("Product Name: " + record.key());
            System.out.println("Quantity: " + record.value());
        }
        consumer.close();
    }
}

