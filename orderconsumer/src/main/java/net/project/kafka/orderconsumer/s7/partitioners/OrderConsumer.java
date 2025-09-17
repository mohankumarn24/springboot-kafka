package net.project.kafka.orderconsumer.s7.partitioners;

import net.project.kafka.orderconsumer.s7.partitioners.model.Order;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup"); // https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"));

        try {
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, Order> record: records) {
                    String customerName = record.key();
                    Order order = record.value();
                    System.out.println(String.format("CustomerName=%s, Product=%s, Quantity=%d, Partition=%d",
                            customerName, order.getProduct(), order.getQuantity(), record.partition()));
                }
            }
        } finally {
            consumer.close();
        }
    }
}