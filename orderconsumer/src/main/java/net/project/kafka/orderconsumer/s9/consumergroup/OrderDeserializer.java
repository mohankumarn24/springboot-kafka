package net.project.kafka.orderconsumer.s9.consumergroup;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.project.kafka.orderconsumer.s9.consumergroup.model.Order;
import org.apache.kafka.common.serialization.Deserializer;
import java.io.IOException;

public class OrderDeserializer implements Deserializer<Order> {

    @Override
    public Order deserialize(String topic, byte[] data) {

        ObjectMapper objectMapper = new ObjectMapper();
        Order order = null;
        try {
            order = objectMapper.readValue(data, Order.class);
        } catch (IOException e) {
            System.out.println("Exception occurred: " + e.getStackTrace());
        }
        return order;
    }
}

