package org.example;

import org.example.kafka.HeartbeatMonitorConsumer;
import org.example.kafka.KlineConsumer;
import org.example.kafka.ProducerStatusConsumer;
import org.example.kafka.TickerConsumer;
import org.example.service.KlineRepository;
import org.example.service.ProducerMonitoringRepository;
import org.example.service.TickerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String KAFKA_BOOTSTRAP_SERVERS   = "localhost:9092";
    private static final String TICKER_CONSUMER_GROUP     = "bainance-consumer-group";
    private static final String KLINE_CONSUMER_GROUP      = "bainance-kline-consumer-group";

    private static final String DB_URL      = "jdbc:mariadb://localhost:33061/bainance?serverTimezone=Asia/Seoul";
    private static final String DB_USER     = "deadde";
    private static final String DB_PASSWORD = "qwer1234";

    public static void main(String[] args) {
        TickerRepository             tickerRepo    = new TickerRepository(DB_URL, DB_USER, DB_PASSWORD);
        KlineRepository              klineRepo     = new KlineRepository(DB_URL, DB_USER, DB_PASSWORD);
        ProducerMonitoringRepository monitoringRepo = new ProducerMonitoringRepository(DB_URL, DB_USER, DB_PASSWORD);

        TickerConsumer          tickerConsumer   = new TickerConsumer(KAFKA_BOOTSTRAP_SERVERS, TICKER_CONSUMER_GROUP, tickerRepo);
        KlineConsumer           klineConsumer    = new KlineConsumer(KAFKA_BOOTSTRAP_SERVERS, KLINE_CONSUMER_GROUP, klineRepo);
        HeartbeatMonitorConsumer heartbeatMonitor = new HeartbeatMonitorConsumer(KAFKA_BOOTSTRAP_SERVERS);
        ProducerStatusConsumer  statusConsumer   = new ProducerStatusConsumer(KAFKA_BOOTSTRAP_SERVERS, monitoringRepo);

        final TickerConsumer             fTickerConsumer   = tickerConsumer;
        final KlineConsumer              fKlineConsumer    = klineConsumer;
        final HeartbeatMonitorConsumer   fHeartbeatMonitor = heartbeatMonitor;
        final ProducerStatusConsumer     fStatusConsumer   = statusConsumer;
        final TickerRepository           fTickerRepo       = tickerRepo;
        final KlineRepository            fKlineRepo        = klineRepo;
        final ProducerMonitoringRepository fMonitoringRepo  = monitoringRepo;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                log.info("Shutting down...");
                fTickerConsumer.close();
                fKlineConsumer.close();
                fHeartbeatMonitor.close();
                fStatusConsumer.close();
                fTickerRepo.close();
                fKlineRepo.close();
                fMonitoringRepo.close();
            }
        }));

        Thread klineThread     = new Thread(klineConsumer,    "kline-consumer");
        Thread heartbeatThread = new Thread(heartbeatMonitor, "heartbeat-monitor");
        Thread statusThread    = new Thread(statusConsumer,   "producer-status-consumer");
        klineThread.start();
        heartbeatThread.start();
        statusThread.start();
        tickerConsumer.run();
    }
}
