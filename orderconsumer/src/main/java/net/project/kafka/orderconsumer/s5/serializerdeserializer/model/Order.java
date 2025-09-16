package net.project.kafka.orderconsumer.s5.serializerdeserializer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private String customerName;
    private String product;
    private int quantity;
}