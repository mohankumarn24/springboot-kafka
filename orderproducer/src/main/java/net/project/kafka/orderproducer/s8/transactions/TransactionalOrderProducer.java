package net.project.kafka.orderproducer.s8.transactions;

import java.util.Properties;

import net.project.kafka.orderproducer.s4.producerconsumer.OrderCallBack;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionalOrderProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1");
        // props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        ProducerRecord<String, Integer> record1 = new ProducerRecord<>("OrderTopicTransactions", "Mac Book Pro", 10);
        ProducerRecord<String, Integer> record2 = new ProducerRecord<>("OrderTopicTransactions", "Dell Laptop", 20);

        try {
            // both transaction should succeed or both should fail
            producer.beginTransaction();
            producer.send(record1, new OrderCallBack());    // transaction 1
            // int x = 5 / 0;                               // RTE
            producer.send(record2);                         // transaction 2
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
