package net.project.kafka.orderconsumer.s6.avro.deserializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GenericOrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "http://172.25.50.202:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url", "http://172.25.50.202:8081");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroGRTopic"));

        try {
            // run infinitely
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    String customerName = record.key();
                    GenericRecord order = record.value();
                    System.out.println(String.format("CustomerName=%s, Product=%s, Quantity=%d", customerName, order.get("product"), order.get("quantity")));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
