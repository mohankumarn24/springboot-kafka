package net.project.kafka.orderconsumer.s5.serializerdeserializer;

import net.project.kafka.orderconsumer.s5.serializerdeserializer.model.Order;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup"); // https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderCSTopic"));

        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(200));
        for (ConsumerRecord<String, Order> record: records) {
            String customerName = record.key();
            Order order = record.value();
            System.out.println("Customer Name: " + customerName);
            System.out.println("Product: " + order.getProduct());
            System.out.println("Quantity: " + order.getQuantity());
        }
        consumer.close();
    }
}
