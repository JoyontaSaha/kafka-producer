package com.joyontasaha.kafkaproducer.controller;

import com.joyontasaha.kafkaproducer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, User> userKafkaTemplate;

    private static final String TOPIC = "Kafka_Example";

    private static final String TOPIC_JSON = "Kafka_Example_Json";

    @GetMapping("/publish/{name}")
    public String publishMessage(@PathVariable("name") String name) {
        String message = name + " is published";
        log.info("message :: {}", message);
        kafkaTemplate.send(TOPIC, message);
        return message;
    }

    @PostMapping("/publish")
    public String publishJsonMessage(@RequestBody User user) {
        String message = user + " is published";
        log.info("user :: {}", user);
        userKafkaTemplate.send(TOPIC_JSON, user);
        return message;
    }
}
