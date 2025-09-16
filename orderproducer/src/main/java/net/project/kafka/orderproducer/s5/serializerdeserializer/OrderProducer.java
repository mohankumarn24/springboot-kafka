package net.project.kafka.orderproducer.s5.serializerdeserializer;

import net.project.kafka.orderproducer.s4.producerconsumer.OrderCallBack;
import net.project.kafka.orderproducer.s5.serializerdeserializer.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "net.project.kafka.orderproducer.s5.serializerdeserializer.OrderSerializer");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Mohan", "iPhone", 1);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", order.getCustomerName(), order);
        // kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic OrderCSTopic
        // kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic OrderPartitionedTopic

        try {
            producer.send(record, new OrderCallBack());
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.getStackTrace());
        } finally {
            producer.close();
        }
    }
}
