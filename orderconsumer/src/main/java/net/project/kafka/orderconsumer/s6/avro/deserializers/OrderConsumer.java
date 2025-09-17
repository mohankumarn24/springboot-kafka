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
        props.setProperty("bootstrap.servers", "http://192.168.1.112:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url", "http://192.168.1.112:8081");
        props.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroTopic"));

        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(200));
        for (ConsumerRecord<String, Order> record : records) {
            String customerName = record.key();
            Order order = record.value();
            System.out.println("Customer Name: " + customerName);
            System.out.println("Product: " + order.getProduct());
            System.out.println("Quantity: " + order.getQuantity());
        }
        consumer.close();
    }
}
