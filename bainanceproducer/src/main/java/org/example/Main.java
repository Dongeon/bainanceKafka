package org.example;

import org.example.client.BinanceKlineWebSocketClient;
import org.example.client.BinanceWebSocketClient;
import org.example.handler.KlineEventHandler;
import org.example.handler.TickerEventHandler;
import org.example.kafka.KafkaKlineProducer;
import org.example.kafka.KafkaTickerProducer;
import org.example.leader.HeartbeatMessage;
import org.example.leader.HeartbeatPublisher;
import org.example.leader.HeartbeatWatcher;
import org.example.leader.LeaderElector;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 바이낸스 Kafka Producer 애플리케이션 진입점.
 *
 * <p>실행 방법:
 * <pre>
 *   ./gradlew run                          # instanceId 자동 생성
 *   ./gradlew run --args="producer-1"      # instanceId=producer-1
 *   ./gradlew run --args="producer-2"      # 두 번째 인스턴스 (별도 터미널)
 * </pre>
 *
 * <p>Active-Standby 리더 선출:
 *   두 인스턴스를 동시에 실행하면 LeaderElector가 하나를 ACTIVE, 나머지를 STANDBY로 결정한다.
 *   ACTIVE 인스턴스만 Binance WebSocket에 연결하여 중복 메시지를 방지한다.
 *   ACTIVE가 종료되면 STANDBY가 약 15초 내에 자동으로 인수한다.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final List<String> SYMBOLS = Arrays.asList(
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
            "XRPUSDT", "DOTUSDT", "AVAXUSDT", "ATOMUSDT", "NEARUSDT"
    );
    private static final List<String> KLINE_INTERVALS = Arrays.asList("1m", "5m", "15m", "1h");

    public static void main(String[] args) throws Exception {
        String instanceId = (args.length > 0 && !args[0].isBlank()) ? args[0]
                : "producer-" + UUID.randomUUID().toString().substring(0, 6);

        log.info("Starting producer: instanceId={}, symbols={}", instanceId, SYMBOLS.size());

        List<String> tickerSymbols = SYMBOLS;
        List<String> klineSymbols  = SYMBOLS;

        // ── Kafka 프로듀서 / 핸들러 ────────────────────────────────────────────
        KafkaTickerProducer tickerKafka   = new KafkaTickerProducer(KAFKA_BOOTSTRAP_SERVERS);
        TickerEventHandler  tickerHandler = new TickerEventHandler(tickerKafka);

        KafkaKlineProducer          klineKafka   = new KafkaKlineProducer(KAFKA_BOOTSTRAP_SERVERS);
        KlineEventHandler           klineHandler = new KlineEventHandler(klineKafka);

        // ── WebSocket 클라이언트 (아직 연결하지 않음 — ACTIVE 전환 시 연결) ─────
        BinanceWebSocketClient      tickerClient = new BinanceWebSocketClient(tickerSymbols, tickerHandler);
        BinanceKlineWebSocketClient klineClient  = new BinanceKlineWebSocketClient(klineSymbols, KLINE_INTERVALS, klineHandler);

        // ── 리더 선출 ─────────────────────────────────────────────────────────
        final LeaderElector[] ref = new LeaderElector[1];
        final String fInstanceId  = instanceId;
        final BinanceWebSocketClient      fTickerClient = tickerClient;
        final BinanceKlineWebSocketClient fKlineClient  = klineClient;

        HeartbeatPublisher publisher = new HeartbeatPublisher(KAFKA_BOOTSTRAP_SERVERS, instanceId);
        HeartbeatWatcher   watcher   = new HeartbeatWatcher(KAFKA_BOOTSTRAP_SERVERS, instanceId,
                new Consumer<HeartbeatMessage>() {
                    public void accept(HeartbeatMessage msg) {
                        ref[0].onForeignHeartbeat(msg);
                    }
                });

        ref[0] = new LeaderElector(
                instanceId,
                publisher,
                watcher,
                new Runnable() {
                    public void run() {
                        log.info("[{}] ACTIVE — Binance WebSocket 연결 시작", fInstanceId);
                        fTickerClient.connect();
                        fKlineClient.connect();
                    }
                },
                new Runnable() {
                    public void run() {
                        log.info("[{}] STANDBY — Binance WebSocket 연결 해제", fInstanceId);
                        fTickerClient.disconnect();
                        fKlineClient.disconnect();
                    }
                }
        );

        // ── 종료 훅 ───────────────────────────────────────────────────────────
        final TickerEventHandler  fTickerHandler = tickerHandler;
        final KafkaTickerProducer fTickerKafka   = tickerKafka;
        final KlineEventHandler   fKlineHandler  = klineHandler;
        final KafkaKlineProducer  fKlineKafka    = klineKafka;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                log.info("[{}] Shutting down...", fInstanceId);
                ref[0].shutdown();
                fTickerHandler.shutdown();
                fTickerKafka.close();
                fKlineHandler.shutdown();
                fKlineKafka.close();
            }
        }));

        ref[0].start();
        Thread.currentThread().join();
    }

}
