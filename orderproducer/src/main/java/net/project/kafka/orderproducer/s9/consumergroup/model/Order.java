package net.project.kafka.orderproducer.s9.consumergroup.model;

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
