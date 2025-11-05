package net.project.kafka.orderconsumer.s6.avro.deserializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import net.project.kafka.s6.avro.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "http://172.25.50.202:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url", "http://172.25.50.202:8081");
        // specific.avro.reader comes into play when using Kafka + Confluent Schema Registry or similar frameworks for deserializing Avro data
        // By setting this property to true, we instruct the KafkaAvroDeserializer to use the generated specific Avro classes (like Order) instead of GenericRecord
        props.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroTopic"));

        try {
            // run infinitely
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, Order> record : records) {
                    String customerName = record.key();
                    Order order = record.value();
                    System.out.println(String.format("CustomerName=%s, Product=%s, Quantity=%d", customerName, order.getProduct(), order.getQuantity()));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
