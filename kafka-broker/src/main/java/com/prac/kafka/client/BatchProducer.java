package com.prac.kafka.client;

import com.prac.kafka.protocol.request.ProduceMessage;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BatchProducer {

    private final BrokerClient client;
    private final BatchAccumulator batchAccumulator;
    private final String topic;
    private final ScheduledExecutorService scheduler;

    public BatchProducer(BrokerClient client, BatchAccumulator batchAccumulator, String topic,
                         ScheduledExecutorService scheduler) {
        this.client = client;
        this.batchAccumulator = batchAccumulator;
        this.topic = topic;
        this.scheduler = scheduler;
    }

    public void send(String key, String value) throws Exception {
        Optional<List<ProduceMessage>> produceMessages = batchAccumulator.add(key, value);
        if (produceMessages.isPresent()) {
            client.batchProduce(topic, produceMessages.get());
        }
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                Optional<List<ProduceMessage>> batch = batchAccumulator.flush();
                if (batch.isPresent()) {
                    client.batchProduce(topic, batch.get());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, batchAccumulator.getLingerMs(), batchAccumulator.getLingerMs(), TimeUnit.MILLISECONDS);
    }

    public void close() throws Exception {
        scheduler.shutdown();
        Optional<List<ProduceMessage>> batch = batchAccumulator.forceFlush();
        if (batch.isPresent()) {
            client.batchProduce(topic, batch.get());
        }
        client.close();
    }
}
