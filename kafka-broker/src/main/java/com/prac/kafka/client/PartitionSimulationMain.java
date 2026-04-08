package com.prac.kafka.client;

import com.prac.kafka.protocol.response.CommitOffsetResponse;
import com.prac.kafka.protocol.response.CreateTopicResponse;
import com.prac.kafka.protocol.response.FetchRecord;
import com.prac.kafka.protocol.response.FetchResponse;
import com.prac.kafka.protocol.response.GetOffsetResponse;
import com.prac.kafka.protocol.response.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionSimulationMain {

    private static final Logger log = LoggerFactory.getLogger(PartitionSimulationMain.class);

    public static void main(String[] args) throws Exception {
        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();

        scenarioA(client);
        scenarioB(client);
        scenarioC(client);
        scenarioD(client);

        client.close();
    }

    /**
     * 시나리오 A: Key 기반 순서 보장
     * 같은 key의 이벤트가 같은 파티션에 모이는지 확인
     */
    private static void scenarioA(BrokerClient client) throws Exception {
        log.info("========== 시나리오 A: Key 기반 순서 보장 ==========");

        CreateTopicResponse topicRes = client.createTopic("payments", 3);
        log.info("토픽 생성: {}, partitions: {}", topicRes.topic(), topicRes.partitions());

        ProduceResponse r1 = client.produce("payments", "order-001", "{\"type\":\"CREATED\"}");
        log.info("order-001 CREATED  → partition={}, offset={}", r1.partition(), r1.offset());

        ProduceResponse r2 = client.produce("payments", "order-002", "{\"type\":\"CREATED\"}");
        log.info("order-002 CREATED  → partition={}, offset={}", r2.partition(), r2.offset());

        ProduceResponse r3 = client.produce("payments", "order-001", "{\"type\":\"PAID\"}");
        log.info("order-001 PAID     → partition={}, offset={}", r3.partition(), r3.offset());

        ProduceResponse r4 = client.produce("payments", "order-001", "{\"type\":\"SHIPPED\"}");
        log.info("order-001 SHIPPED  → partition={}, offset={}", r4.partition(), r4.offset());

        ProduceResponse r5 = client.produce("payments", "order-002", "{\"type\":\"PAID\"}");
        log.info("order-002 PAID     → partition={}, offset={}", r5.partition(), r5.offset());

        // order-001이 있는 파티션에서 fetch
        log.info("--- order-001 파티션({})에서 fetch ---", r1.partition());
        FetchResponse fetchRes = client.fetch("payments", r1.partition(), 0, 10);
        for (FetchRecord record : fetchRes.records()) {
            log.info("  offset={}, key={}, value={}", record.offset(), record.key(), record.value());
        }

        log.info("[검증] order-001의 3개 이벤트가 모두 같은 파티션에 있는가? → {}", r1.partition() == r3.partition() && r3.partition() == r4.partition() ? "YES" : "NO");
        log.info("[검증] order-001 파티션={}, order-002 파티션={}", r1.partition(), r2.partition());
    }

    /**
     * 시나리오 B: key=null 라운드로빈
     * key 없이 보내면 파티션이 라운드로빈으로 분배되는지 확인
     */
    private static void scenarioB(BrokerClient client) throws Exception {
        log.info("========== 시나리오 B: key=null 라운드로빈 ==========");

        CreateTopicResponse topicRes = client.createTopic("logs", 3);
        log.info("토픽 생성: {}, partitions: {}", topicRes.topic(), topicRes.partitions());

        for (int i = 1; i <= 6; i++) {
            ProduceResponse res = client.produce("logs", null, "{\"log\":\"event" + i + "\"}");
            log.info("event{} → partition={}, offset={}", i, res.partition(), res.offset());
        }

        log.info("[검증] key=null이면 파티션이 라운드로빈으로 분배되는가? (0→1→2→0→1→2 패턴 확인)");
    }

    /**
     * 시나리오 C: 파티션별 독립 offset
     * 각 파티션의 offset이 독립적으로 관리되는지 확인
     */
    private static void scenarioC(BrokerClient client) throws Exception {
        log.info("========== 시나리오 C: 파티션별 독립 offset ==========");

        CreateTopicResponse topicRes = client.createTopic("orders", 2);
        log.info("토픽 생성: {}, partitions: {}", topicRes.topic(), topicRes.partitions());

        // 같은 key로 보내서 한 파티션에 2개 쌓기
        ProduceResponse r1 = client.produce("orders", "key-a", "{\"data\":\"a1\"}");
        log.info("key-a → partition={}, offset={}", r1.partition(), r1.offset());

        ProduceResponse r2 = client.produce("orders", "key-a", "{\"data\":\"a2\"}");
        log.info("key-a → partition={}, offset={}", r2.partition(), r2.offset());

        // 다른 key로 보내서 다른 파티션에 쌓기 (해시에 따라 다를 수 있음)
        ProduceResponse r3 = client.produce("orders", "key-b", "{\"data\":\"b1\"}");
        log.info("key-b → partition={}, offset={}", r3.partition(), r3.offset());

        // 각 파티션에서 fetch
        log.info("--- partition 0 fetch ---");
        FetchResponse fetch0 = client.fetch("orders", 0, 0, 10);
        for (FetchRecord record : fetch0.records()) {
            log.info("  offset={}, key={}, value={}", record.offset(), record.key(), record.value());
        }

        log.info("--- partition 1 fetch ---");
        FetchResponse fetch1 = client.fetch("orders", 1, 0, 10);
        for (FetchRecord record : fetch1.records()) {
            log.info("  offset={}, key={}, value={}", record.offset(), record.key(), record.value());
        }

        log.info("[검증] 파티션마다 offset이 0부터 독립적으로 시작하는가?");
    }

    /**
     * 시나리오 D: 파티션별 offset 커밋
     * 같은 Consumer가 파티션별로 서로 다른 offset을 커밋할 수 있는지 확인
     */
    private static void scenarioD(BrokerClient client) throws Exception {
        log.info("========== 시나리오 D: 파티션별 offset 커밋 ==========");

        String consumerId = "consumer-1";
        String topic = "payments";

        // partition 0에서 fetch 후 커밋
        FetchResponse fetch0 = client.fetch(topic, 0, 0, 10);
        log.info("partition 0: {}개 수신", fetch0.records().size());
        if (!fetch0.records().isEmpty()) {
            CommitOffsetResponse commitRes = client.commitOffset(consumerId, topic, 0, fetch0.nextOffset());
            log.info("partition 0: offset {} 커밋", commitRes.offset());
        }

        // partition 1에서 fetch 후 커밋
        FetchResponse fetch1 = client.fetch(topic, 1, 0, 10);
        log.info("partition 1: {}개 수신", fetch1.records().size());
        if (!fetch1.records().isEmpty()) {
            CommitOffsetResponse commitRes = client.commitOffset(consumerId, topic, 1, fetch1.nextOffset());
            log.info("partition 1: offset {} 커밋", commitRes.offset());
        }

        // 파티션별 committed offset 조회
        GetOffsetResponse offset0 = client.getCommittedOffset(consumerId, topic, 0);
        GetOffsetResponse offset1 = client.getCommittedOffset(consumerId, topic, 1);
        log.info("partition 0 committed offset: {}", offset0.committed());
        log.info("partition 1 committed offset: {}", offset1.committed());

        log.info("[검증] 같은 Consumer가 파티션별로 서로 다른 offset을 커밋할 수 있는가?");
    }
}
