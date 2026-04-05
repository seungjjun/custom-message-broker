package com.prac.kafka.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.consumer.OffsetManager;
import com.prac.kafka.handler.CommitOffsetHandler;
import com.prac.kafka.handler.CreateTopicHandler;
import com.prac.kafka.handler.FetchHandler;
import com.prac.kafka.handler.GetOffsetHandler;
import com.prac.kafka.handler.ProduceHandler;
import com.prac.kafka.storage.TopicManager;

public class MainApplication {

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();
        TopicManager topicManager = new TopicManager();
        OffsetManager offsetManager = new OffsetManager();

        ProduceHandler produceHandler = new ProduceHandler(topicManager, objectMapper);
        FetchHandler fetchHandler = new FetchHandler(topicManager, objectMapper);
        CreateTopicHandler createTopicHandler = new CreateTopicHandler(topicManager, objectMapper);
        CommitOffsetHandler commitOffsetHandler = new CommitOffsetHandler(offsetManager, objectMapper);
        GetOffsetHandler getOffsetHandler = new GetOffsetHandler(offsetManager, objectMapper);

        new KafkaBrokerServer(9092, produceHandler, fetchHandler, createTopicHandler, commitOffsetHandler, getOffsetHandler).start();
    }
}
