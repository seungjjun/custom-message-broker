package com.prac.kafka.bootstrap;

public class MainApplication {

    public static void main(String[] args) throws InterruptedException {
        new KafkaBrokerServer(9092).start();
    }
}
