package net.project.kafka.orderconsumer.s8.transactions;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TransactionalOrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderTopicTransactions"));

        ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, Integer> record: records) {
            System.out.println(String.format("ProductName=%s, Quantity=%d", record.key(), record.value()));
        }
        consumer.close();
    }
}