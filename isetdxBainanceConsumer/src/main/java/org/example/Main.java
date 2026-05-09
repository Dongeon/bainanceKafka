package org.example;

import org.example.config.AppConfig;
import org.example.consumer.KlineConsumer;
import org.example.consumer.TickerConsumer;
import org.example.kafka.consumer.KafkaApplication;
import org.example.repository.KlineRepository;
import org.example.repository.TickerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main extends KafkaApplication {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public Main() throws IOException { super(); }

    @Override
    public void run() throws Exception {
        AppConfig appConfig = AppConfig.load("kafka.conf");

        TickerRepository tickerRepo = new TickerRepository(
                appConfig.bainanceDbUrl(), appConfig.bainanceDbUser(), appConfig.bainanceDbPassword());
        KlineRepository klineRepo = new KlineRepository(
                appConfig.bainanceDbUrl(), appConfig.bainanceDbUser(), appConfig.bainanceDbPassword());

        TickerConsumer tickerConsumer = new TickerConsumer(appConfig, tickerRepo);
        KlineConsumer  klineConsumer  = new KlineConsumer(appConfig, klineRepo);

        final TickerConsumer   fTicker     = tickerConsumer;
        final KlineConsumer    fKline      = klineConsumer;
        final TickerRepository fTickerRepo = tickerRepo;
        final KlineRepository  fKlineRepo  = klineRepo;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                fTicker.close();
                fKline.close();
                fTickerRepo.close();
                fKlineRepo.close();
            }
        }));

        new Thread(tickerConsumer, "ticker-consumer").start();
        new Thread(klineConsumer,  "kline-consumer").start();

        Thread.currentThread().join();
    }

    public static void main(String[] args) throws Exception {
        new Main().run();
    }
}
