package net.project.kafka.userproducer.service;

import net.project.kafka.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserProducerService {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public void sendUserData(User user) {
        kafkaTemplate.send("user-topic", user.getName(), user);
    }
}