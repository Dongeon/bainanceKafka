package org.example;

import org.example.kafka.consumer.KafkaApplication;

import java.io.IOException;

public class Main extends KafkaApplication {

    public Main() throws IOException { super(); }

    @Override
    public void run() throws Exception {
        Thread.currentThread().join();
    }

    public static void main(String[] args) throws Exception {
        new Main().run();
    }
}
