package net.project.kafka.orderproducer.s7.partitioners;

import net.project.kafka.orderproducer.s4.producerconsumer.OrderCallBack;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class OrderProducerProps {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        // additional props
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "343434334");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "2");
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "400");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1024");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopicProps", "Mac Book Pro", 10);
        try {
            producer.send(record, new OrderCallBack());
            System.out.println("Message sent successfully");
        } finally {
            producer.close();
        }
    }
}
