package net.project.kafka.userproducer.controller;

import net.project.kafka.model.User;
import net.project.kafka.userproducer.service.UserProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/users")
public class UserController {

    @Autowired
    private UserProducerService service;

    @PostMapping("/publish")
    public void sendUserData(@RequestBody User user) {
        service.sendUserData(user);
    }
}

/*
cd C:\dev\kafka_2.12-3.4.0
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties


POST: http://localhost:8080/api/v1/users/publish
{
  "name" : "Mohan",
  "genre" : "Action"
}
 */