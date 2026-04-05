package com.prac.kafka.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.handler.CreateTopicHandler;
import com.prac.kafka.handler.FetchHandler;
import com.prac.kafka.handler.ProduceHandler;
import com.prac.kafka.storage.TopicManager;

public class MainApplication {

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();
        TopicManager topicManager = new TopicManager();
        ProduceHandler produceHandler = new ProduceHandler(topicManager, objectMapper);
        FetchHandler fetchHandler = new FetchHandler(topicManager, objectMapper);
        CreateTopicHandler createTopicHandler = new CreateTopicHandler(topicManager, objectMapper);

        new KafkaBrokerServer(9092, produceHandler, fetchHandler, createTopicHandler).start();
    }
}
