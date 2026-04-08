package com.prac.kafka.client;

import com.prac.kafka.protocol.response.FetchRecord;
import com.prac.kafka.protocol.response.FetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) throws Exception {
        BrokerClient client = new BrokerClient("localhost", 9092);
        client.connect();

        FetchResponse res1 = client.fetch("payments", 0, 0, 10);
        log.info("fetch 1 - partition=0, {}개 수신, nextOffset: {}", res1.records().size(), res1.nextOffset());
        for (FetchRecord record : res1.records()) {
            log.info("  offset={}, key={}, value={}", record.offset(), record.key(), record.value());
        }

        FetchResponse res2 = client.fetch("payments", 0, 0, 10);
        log.info("fetch 2 - partition=0, {}개 수신 (불변성 확인)", res2.records().size());

        FetchResponse res3 = client.fetch("payments", 0, 2, 10);
        log.info("fetch 3 - partition=0, offset=2부터 {}개 수신", res3.records().size());
        for (FetchRecord record : res3.records()) {
            log.info("  offset={}, key={}, value={}", record.offset(), record.key(), record.value());
        }

        client.close();
    }
}
