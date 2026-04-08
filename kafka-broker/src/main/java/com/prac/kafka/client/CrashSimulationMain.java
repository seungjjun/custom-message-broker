package com.prac.kafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrashSimulationMain {

    private static final Logger log = LoggerFactory.getLogger(CrashSimulationMain.class);

    public static void main(String[] args) throws Exception {
        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();

        log.info("========== At-most-once crash 시뮬레이션 ==========");
        simulateAtMostOnce(client);

        log.info("========== At-least-once crash 시뮬레이션 ==========");
        simulateAtLeastOnce(client);

        client.close();
    }

    private static void simulateAtMostOnce(BrokerClient client) throws Exception {
        CommitStrategyConsumer consumer = new CommitStrategyConsumer(client, "at-most-once-consumer", "payments", 0, 3);
        consumer.setCrashAtOffset(1);

        // 1회차: 처리 중 crash
        log.info("--- 1회차: 소비 시작 ---");
        try {
            consumer.consumeAtMostOnce();
        } catch (RuntimeException e) {
            log.error("crash 발생: {}", e.getMessage());
        }

        // 2회차: 재시작 후 확인
        log.info("--- 2회차: 재시작 후 소비 ---");
        consumer.setCrashAtOffset(-1); // crash 해제
        consumer.consumeAtMostOnce();

        long committed = client.getCommittedOffset("at-most-once-consumer", "payments", 0).committed();
        log.info("결과: committed offset={}, 레코드 1, 2는 처리되지 않았지만 커밋됨 → 유실!", committed);
    }

    private static void simulateAtLeastOnce(BrokerClient client) throws Exception {
        CommitStrategyConsumer consumer = new CommitStrategyConsumer(client, "at-least-once-consumer", "payments", 0, 3);
        consumer.setCrashAtOffset(1);

        // 1회차: 처리 중 crash
        log.info("--- 1회차: 소비 시작 ---");
        try {
            consumer.consumeAtLeastOnce();
        } catch (RuntimeException e) {
            log.error("crash 발생: {}", e.getMessage());
        }

        // 2회차: 재시작 후 확인
        log.info("--- 2회차: 재시작 후 소비 ---");
        consumer.setCrashAtOffset(-1); // crash 해제
        consumer.consumeAtLeastOnce();

        long committed = client.getCommittedOffset("at-least-once-consumer", "payments", 0).committed();
        log.info("결과: committed offset={}, 레코드 0은 다시 처리됨 → 중복! 하지만 유실 없음", committed);
    }
}
