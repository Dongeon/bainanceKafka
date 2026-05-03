package org.example;

import org.example.client.BinanceKlineWebSocketClient;
import org.example.client.BinanceWebSocketClient;
import org.example.handler.KlineEventHandler;
import org.example.handler.TickerEventHandler;
import org.example.kafka.KafkaKlineProducer;
import org.example.kafka.KafkaTickerProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 바이낸스 Kafka Producer 애플리케이션 진입점.
 *
 * <p>실행 방법:
 * <pre>
 *   # Kafka와 Zookeeper가 먼저 실행되어 있어야 한다.
 *   alias kafka-zoo='~/projects/kafka/bin/zookeeper-server-start.sh ~/projects/kafka/config/zookeeper.properties'
 *   alias kafka-start='~/projects/kafka/bin/kafka-server-start.sh ~/projects/kafka/config/server.properties'
 *   alias kafka-producer='cd ~/projects/java_bainance/bainanceproducer && ./gradlew run'
 * </pre>
 *
 * <p>의존 서비스:
 *   - Kafka 브로커: localhost:9092
 *   - Kafka 토픽: crypto-ticker, crypto-kline (사전 생성 필요)
 *   - 인터넷 연결: wss://stream.binance.com:9443
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws InterruptedException {
        List<String> symbols = List.of(
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
                "XRPUSDT", "DOTUSDT", "AVAXUSDT", "ATOMUSDT", "NEARUSDT"
        );
        List<String> klineIntervals = List.of("1m", "5m", "15m", "1h");

        // ── Ticker (기존) ────────────────────────────────────────────────────
        KafkaTickerProducer tickerKafka   = new KafkaTickerProducer(KAFKA_BOOTSTRAP_SERVERS);
        TickerEventHandler  tickerHandler = new TickerEventHandler(tickerKafka);
        BinanceWebSocketClient tickerClient = new BinanceWebSocketClient(symbols, tickerHandler);

        // ── Kline (신규) ─────────────────────────────────────────────────────
        KafkaKlineProducer  klineKafka   = new KafkaKlineProducer(KAFKA_BOOTSTRAP_SERVERS);
        KlineEventHandler   klineHandler = new KlineEventHandler(klineKafka);
        BinanceKlineWebSocketClient klineClient =
                new BinanceKlineWebSocketClient(symbols, klineIntervals, klineHandler);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            tickerClient.disconnect();
            tickerHandler.shutdown();
            tickerKafka.close();
            klineClient.disconnect();
            klineHandler.shutdown();
            klineKafka.close();
        }));

        tickerClient.connect();
        klineClient.connect();

        Thread.currentThread().join();
    }
}
