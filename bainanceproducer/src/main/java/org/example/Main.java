package org.example;

import org.example.client.BinanceKlineWebSocketClient;
import org.example.client.BinanceSymbolFetcher;
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

import java.util.List;
import java.util.UUID;

/**
 * 바이낸스 Kafka Producer 애플리케이션 진입점.
 *
 * <p>실행 방법:
 * <pre>
 *   ./gradlew run --args="50"              # 심볼 50개, instanceId 자동 생성
 *   ./gradlew run --args="50 producer-1"   # 심볼 50개, instanceId=producer-1
 *   ./gradlew run --args="50 producer-2"   # 두 번째 인스턴스 (별도 터미널)
 * </pre>
 *
 * <p>Active-Standby 리더 선출:
 *   두 인스턴스를 동시에 실행하면 LeaderElector가 하나를 ACTIVE, 나머지를 STANDBY로 결정한다.
 *   ACTIVE 인스턴스만 Binance WebSocket에 연결하여 중복 메시지를 방지한다.
 *   ACTIVE가 종료되면 STANDBY가 약 15초 내에 자동으로 인수한다.
 *
 * <p>WebSocket 스트림 한계 (연결당 최대 1024 스트림):
 *   - Ticker:  심볼 수 그대로 → 최대 1024 심볼
 *   - Kline:   심볼 × 4 인터벌 → 최대 256 심볼 (256 × 4 = 1024)
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final int DEFAULT_SYMBOL_COUNT     = 10;
    private static final int TICKER_WS_MAX            = 1024;
    private static final List<String> KLINE_INTERVALS = List.of("1m", "5m", "15m", "1h");
    private static final int KLINE_WS_MAX             = TICKER_WS_MAX / KLINE_INTERVALS.size();

    public static void main(String[] args) throws Exception {
        int    symbolCount = parseSymbolCount(args);
        String instanceId  = parseInstanceId(args);

        log.info("Starting producer: instanceId={}, symbols={}", instanceId, symbolCount);

        // ── 심볼 목록 ─────────────────────────────────────────────────────────
        List<String> symbols = symbolCount > 660
                ? BinanceSymbolFetcher.fetchAllSymbols(symbolCount)
                : BinanceSymbolFetcher.fetchUsdtSymbols(symbolCount);

        List<String> tickerSymbols = capSymbols(symbols, TICKER_WS_MAX, "Ticker");
        List<String> klineSymbols  = capSymbols(symbols, KLINE_WS_MAX,  "Kline");

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

    private static int parseSymbolCount(String[] args) {
        if (args.length == 0) {
            log.info("심볼 수 인수 없음 → 기본값 {}개 사용", DEFAULT_SYMBOL_COUNT);
            return DEFAULT_SYMBOL_COUNT;
        }
        try {
            int count = Integer.parseInt(args[0]);
            if (count <= 0) throw new IllegalArgumentException("0 이하");
            return count;
        } catch (Exception e) {
            log.warn("잘못된 심볼 수 '{}' → 기본값 {}개 사용", args[0], DEFAULT_SYMBOL_COUNT);
            return DEFAULT_SYMBOL_COUNT;
        }
    }

    private static String parseInstanceId(String[] args) {
        if (args.length >= 2 && !args[1].isBlank()) return args[1];
        return "producer-" + UUID.randomUUID().toString().substring(0, 6);
    }

    private static List<String> capSymbols(List<String> symbols, int max, String label) {
        if (symbols.size() <= max) return symbols;
        log.warn("[{}] 심볼 {}개가 WS 한계 {}개를 초과 → {}개로 제한", label, symbols.size(), max, max);
        return symbols.subList(0, max);
    }
}
