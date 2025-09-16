package net.project.kafka.orderproducer.s4.producerconsumer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

public class OrderCallBack implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        System.out.println("Partition: " + metadata.partition());
        System.out.println("Offset: " + metadata.offset());
        System.out.println("Message sent successfully");

        if (exception != null) {
            System.out.println("Exception occurred: " + exception.getMessage());
        }
    }
}

