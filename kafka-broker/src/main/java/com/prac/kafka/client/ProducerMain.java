package com.prac.kafka.client;

import com.prac.kafka.protocol.response.CreateTopicResponse;
import com.prac.kafka.protocol.response.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {

    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) throws Exception {
        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();

        CreateTopicResponse topicRes = client.createTopic("payments", 3);
        log.info("토픽 생성: {}, partitions: {}", topicRes.topic(), topicRes.partitions());

        ProduceResponse res1 = client.produce("payments", "order-001", "{\"amount\":5000}");
        log.info("produce offset: {}", res1.offset());

        ProduceResponse res2 = client.produce("payments", "order-002", "{\"amount\":3000}");
        log.info("produce offset: {}", res2.offset());

        ProduceResponse res3 = client.produce("payments", "order-003", "{\"amount\":7000}");
        log.info("produce offset: {}", res3.offset());

        client.close();
    }
}
