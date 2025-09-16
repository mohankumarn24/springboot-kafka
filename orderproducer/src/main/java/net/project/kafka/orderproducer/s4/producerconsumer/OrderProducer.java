package net.project.kafka.orderproducer.s4.producerconsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        // 1. Fire and forget
        /*
        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
        try {
            producer.send(record);
            System.out.println("Message sent successfully");
        } catch (Exception e) {
            System.out.println("Error occurred: " + e.getMessage());
        } finally {
            producer.close();
        }
        */

        // 2. sync call
        /*
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("Partition: " + metadata.partition());
            System.out.println("Offset: " + metadata.offset());
            System.out.println("Message sent successfully");
        } catch (Exception e) {
            System.out.println("Error occurred: " + e.getMessage());
        }
        */

        // 3. async call. Method will not wait. Whenever there is a response, onCompletion method is called in OrderCallBack class
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
            producer.send(record, new OrderCallBack());
        } catch (Exception e) {
            System.out.println(new StringBuilder().append("Exception occurred: ").append(e.getStackTrace()).toString());
        }
    }
}
