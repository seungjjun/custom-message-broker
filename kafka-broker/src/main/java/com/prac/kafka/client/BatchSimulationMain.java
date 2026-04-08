package com.prac.kafka.client;

import com.prac.kafka.protocol.response.FetchRecord;
import com.prac.kafka.protocol.response.FetchResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchSimulationMain {

    private static final Logger log = LoggerFactory.getLogger(BatchSimulationMain.class);
    private static final int MESSAGE_COUNT = 1000;

    public static void main(String[] args) throws Exception {
        scenarioA();
        scenarioB();
        scenarioC();
        scenarioD();
    }

    /**
     * 시나리오 A: 배치 vs 건건 전송 성능 비교
     */
    private static void scenarioA() throws Exception {
        log.info("========== 시나리오 A: 배치 vs 건건 전송 성능 비교 ==========");

        // 건건 전송 (batchSize=1, lingerMs=0)
        {
            BrokerClient client = new BrokerClient("localhost", 9092);
            client.connect();
            client.createTopic("bench-single", 3);

            long start = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                client.produce("bench-single", "key-" + i, "{\"data\":" + i + "}");
            }
            long elapsed = System.currentTimeMillis() - start;

            log.info("[건건 전송] {}개 메시지, 네트워크 요청 {}번, 소요 시간: {}ms",
                MESSAGE_COUNT, MESSAGE_COUNT, elapsed);
            client.close();
        }

        // 배치 전송 (batchSize=100, lingerMs=10)
        {
            BrokerClient client = new BrokerClient("localhost", 9092);
            client.connect();
            client.createTopic("bench-batch", 3);

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            BatchAccumulator accumulator = new BatchAccumulator(100, 10);
            BatchProducer producer = new BatchProducer(client, accumulator, "bench-batch", scheduler);
            producer.start();

            long start = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                producer.send("key-" + i, "{\"data\":" + i + "}");
            }
            producer.close();
            long elapsed = System.currentTimeMillis() - start;

            log.info("[배치 전송] {}개 메시지, 네트워크 요청 ~{}번, 소요 시간: {}ms",
                MESSAGE_COUNT, MESSAGE_COUNT / 100, elapsed);
        }
    }

    /**
     * 시나리오 B: batchSize 트리거 확인
     * batchSize=5, lingerMs=5000 (충분히 길게)
     * 5번째 send() 시점에 lingerMs를 기다리지 않고 즉시 전송되는지 확인
     */
    private static void scenarioB() throws Exception {
        log.info("========== 시나리오 B: batchSize 트리거 확인 ==========");

        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();
        client.createTopic("batch-size-test", 1);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        BatchAccumulator accumulator = new BatchAccumulator(5, 5000);
        BatchProducer producer = new BatchProducer(client, accumulator, "batch-size-test", scheduler);
        producer.start();

        for (int i = 1; i <= 5; i++) {
            log.info("send #{} (버퍼: {}/5)", i, i);
            producer.send("k" + i, "v" + i);
        }

        // batchSize 도달로 즉시 전송되었는지 fetch로 확인
        FetchResponse fetchRes = client.fetch("batch-size-test", 0, 0, 10);
        log.info("[검증] batchSize 도달 즉시 전송 → fetch 결과: {}건 수신", fetchRes.records().size());
        log.info("[검증] lingerMs(5초)를 기다리지 않고 즉시 전송되었는가? → {}",
            fetchRes.records().size() == 5 ? "YES" : "NO");

        producer.close();
    }

    /**
     * 시나리오 C: lingerMs 트리거 확인
     * batchSize=100 (충분히 크게), lingerMs=50
     * batchSize에 도달하지 않아도 lingerMs 후에 전송되는지 확인
     */
    private static void scenarioC() throws Exception {
        log.info("========== 시나리오 C: lingerMs 트리거 확인 ==========");

        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();
        client.createTopic("linger-test", 1);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        BatchAccumulator accumulator = new BatchAccumulator(100, 50);
        BatchProducer producer = new BatchProducer(client, accumulator, "linger-test", scheduler);
        producer.start();

        producer.send("k1", "v1");
        producer.send("k2", "v2");
        producer.send("k3", "v3");
        log.info("3건 send 완료 (batchSize=100이므로 아직 버퍼에 있음)");

        // lingerMs(50ms) + 여유 대기
        Thread.sleep(200);

        FetchResponse fetchRes = client.fetch("linger-test", 0, 0, 10);
        log.info("[검증] lingerMs 만료 후 전송 → fetch 결과: {}건 수신", fetchRes.records().size());
        log.info("[검증] batchSize 미달이지만 lingerMs 후에 전송되었는가? → {}",
            fetchRes.records().size() == 3 ? "YES" : "NO");

        producer.close();
    }

    /**
     * 시나리오 D: close() 시 잔여 버퍼 flush
     * batchSize=100, lingerMs=5000
     * close() 호출 시 버퍼에 남은 메시지가 유실되지 않는지 확인
     */
    private static void scenarioD() throws Exception {
        log.info("========== 시나리오 D: close() 시 잔여 버퍼 flush ==========");

        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();
        client.createTopic("close-test", 1);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        BatchAccumulator accumulator = new BatchAccumulator(100, 5000);
        BatchProducer producer = new BatchProducer(client, accumulator, "close-test", scheduler);
        producer.start();

        producer.send("k1", "v1");
        producer.send("k2", "v2");
        log.info("2건 send 완료 (batchSize=100, lingerMs=5초이므로 버퍼에 남아있음)");

        producer.close();
        log.info("close() 호출 → 잔여 버퍼 flush");

        // close()가 client도 닫으므로 새 클라이언트로 확인
        BrokerClient verifyClient = new BrokerClient("localhost", 9092);
        verifyClient.connect();

        FetchResponse fetchRes = verifyClient.fetch("close-test", 0, 0, 10);
        log.info("[검증] close() 후 fetch 결과: {}건 수신", fetchRes.records().size());
        for (FetchRecord record : fetchRes.records()) {
            log.info("  offset={}, key={}, value={}", record.offset(), record.key(), record.value());
        }
        log.info("[검증] close() 시 버퍼에 남은 메시지가 유실되지 않았는가? → {}",
            fetchRes.records().size() == 2 ? "YES" : "NO");

        verifyClient.close();
    }
}
