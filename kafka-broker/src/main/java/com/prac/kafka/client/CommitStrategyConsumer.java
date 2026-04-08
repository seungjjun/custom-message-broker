package com.prac.kafka.client;

import com.prac.kafka.protocol.response.FetchRecord;
import com.prac.kafka.protocol.response.FetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitStrategyConsumer {

    private static final Logger log = LoggerFactory.getLogger(CommitStrategyConsumer.class);

    private final BrokerClient client;
    private final String consumerId;
    private final String topic;
    private final int partition;
    private final int maxRecords;
    private long crashAtOffset = -1;

    public CommitStrategyConsumer(BrokerClient client, String consumerId, String topic, int partition, int maxRecords) {
        this.client = client;
        this.consumerId = consumerId;
        this.topic = topic;
        this.partition = partition;
        this.maxRecords = maxRecords;
    }

    public void setCrashAtOffset(long offset) {
        this.crashAtOffset = offset;
    }

    public void consumeAtMostOnce() throws Exception {
        long offset = client.getCommittedOffset(consumerId, topic, partition).committed();
        FetchResponse response = client.fetch(topic, partition, offset, maxRecords);

        // 처리 전에 커밋
        client.commitOffset(consumerId, topic, partition, offset + response.records().size());

        for (FetchRecord record : response.records()) {
            process(record);
        }
    }

    public void consumeAtLeastOnce() throws Exception {
        long offset = client.getCommittedOffset(consumerId, topic, partition).committed();
        FetchResponse response = client.fetch(topic, partition, offset, maxRecords);

        // 전부 처리하고
        for (FetchRecord record : response.records()) {
            process(record);
        }

        // 그 다음 커밋
        client.commitOffset(consumerId, topic, partition, offset + response.records().size());
    }

    private void process(FetchRecord record) {
        if (record.offset() == crashAtOffset) {
            throw new RuntimeException("crash! offset=" + record.offset() + " 처리 중 서버 다운");
        }
        log.info("[{}] 처리 중: offset={}, key={}, value={}", consumerId, record.offset(), record.key(), record.value());
    }
}
