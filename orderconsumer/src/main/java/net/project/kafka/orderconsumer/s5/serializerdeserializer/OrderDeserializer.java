package net.project.kafka.orderconsumer.s5.serializerdeserializer;

import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.project.kafka.orderconsumer.s5.serializerdeserializer.model.Order;
import org.apache.kafka.common.serialization.Deserializer;

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

