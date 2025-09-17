package net.project.kafka.userconsumer.service;

import net.project.kafka.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserConsumerService {

    public UserConsumerService() {
    }

    @KafkaListener(topics = {"user-topic"})
    public void consumerUserData(User user) {
        /*
        PrintStream var10000 = System.out;
        String var10001 = user.getName();
        var10000.println("UserName=" + var10001 + ", FavGenre=" + user.getFavGenre());
        */
        System.out.println(String.format("Name=%s, Genre=%s", user.getName(), user.getGenre()));
    }
}