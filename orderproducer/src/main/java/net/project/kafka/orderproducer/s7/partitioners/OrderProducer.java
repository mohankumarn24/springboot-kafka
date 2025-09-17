package net.project.kafka.orderproducer.s7.partitioners;

import net.project.kafka.orderproducer.s4.producerconsumer.OrderCallBack;
import net.project.kafka.orderproducer.s7.partitioners.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "net.project.kafka.orderproducer.s5.serializerdeserializer.OrderSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "net.project.kafka.orderproducer.s7.partitioners.OrderSerializer");
        props.setProperty("partitioner.class", VIPPartitioner.class.getName());

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Mohan", "iPhone", 1);
        // kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic OrderPartitionedTopic
        // kafka-topics --describe --bootstrap-server localhost:9092 --topic OrderPartitionedTopic
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderPartitionedTopic", order.getCustomerName(), order);

        try {
            producer.send(record, new OrderCallBack());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}