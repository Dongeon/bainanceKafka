package org.example;

import org.example.client.BinanceKlineWebSocketClient;
import org.example.client.BinanceSymbolFetcher;
import org.example.client.BinanceWebSocketClient;
import org.example.handler.KlineEventHandler;
import org.example.handler.TickerEventHandler;
import org.example.kafka.leader.LeaderElector;
import org.example.kafka.producer.GenericKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * isetdxKafkaProducer 진입점.
 *
 * <p>실행 방법:
 * <pre>
 *   ./gradlew run                              # kafka.conf 기본값 사용
 *   ./gradlew run --args="kafka.conf"          # 설정 파일 명시
 *   ./gradlew run --args="kafka.conf producer-2 80"  # 설정파일 instanceId weight 오버라이드
 * </pre>
 *
 * <p>Active-Standby 실행:
 * <pre>
 *   터미널 1: ./gradlew run --args="kafka.conf producer-1 100"
 *   터미널 2: ./gradlew run --args="kafka.conf producer-2 80"
 * </pre>
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final int    TICKER_WS_MAX    = 1024;
    private static final String DEFAULT_CONFIG   = "kafka.conf";

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG;

        // kafka.conf 로드 (Binance 전용 설정 포함)
        Properties conf = loadProperties(configPath);

        // args로 instanceId / weight 오버라이드 가능 (두 인스턴스 실행 시 편의)
        if (args.length >= 2 && !args[1].isBlank()) conf.setProperty("leader.instance.id",     args[1]);
        if (args.length >= 3 && !args[2].isBlank()) conf.setProperty("leader.instance.weight", args[2]);

        String instanceId = conf.getProperty("leader.instance.id", "producer-?");
        log.info("Starting isetdxKafkaProducer: config={}, instanceId={}, weight={}",
                configPath, instanceId, conf.getProperty("leader.instance.weight", "100"));

        // ── Binance 설정 읽기 ────────────────────────────────────────────────
        int          symbolCount  = Integer.parseInt(conf.getProperty("binance.symbol.count",    "10"));
        List<String> intervals    = Arrays.asList(conf.getProperty("binance.kline.intervals",   "1m,5m,15m,1h").split(","));
        String       tickerTopic  = conf.getProperty("binance.ticker.topic", "crypto-ticker");
        String       klineTopic   = conf.getProperty("binance.kline.topic",  "crypto-kline");
        int          klineWsMax   = TICKER_WS_MAX / intervals.size();

        // ── 심볼 목록 ────────────────────────────────────────────────────────
        List<String> symbols = symbolCount > 660
                ? BinanceSymbolFetcher.fetchAllSymbols(symbolCount)
                : BinanceSymbolFetcher.fetchUsdtSymbols(symbolCount);

        List<String> tickerSymbols = cap(symbols, TICKER_WS_MAX, "Ticker");
        List<String> klineSymbols  = cap(symbols, klineWsMax,    "Kline");

        // ── IsetDx_kafka-producer-lib ────────────────────────────────────────
        GenericKafkaProducer producer = GenericKafkaProducer.fromConfig(configPath);

        // ── 핸들러 ───────────────────────────────────────────────────────────
        TickerEventHandler tickerHandler = new TickerEventHandler(producer, tickerTopic);
        KlineEventHandler  klineHandler  = new KlineEventHandler(producer,  klineTopic);

        // ── WebSocket 클라이언트 (ACTIVE 전환 시에만 connect) ─────────────────
        BinanceWebSocketClient      tickerClient = new BinanceWebSocketClient(tickerSymbols, tickerHandler);
        BinanceKlineWebSocketClient klineClient  = new BinanceKlineWebSocketClient(klineSymbols, intervals, klineHandler);

        // ── IsetDx_kafka-leader-lib ──────────────────────────────────────────
        final BinanceWebSocketClient      fTickerClient  = tickerClient;
        final BinanceKlineWebSocketClient fKlineClient   = klineClient;
        final String                      fInstanceId    = instanceId;

        LeaderElector elector = LeaderElector.fromConfig(configPath)
                .onActivate(new Runnable() {
                    public void run() {
                        log.info("[{}] ACTIVE — Binance WebSocket 연결 시작", fInstanceId);
                        fTickerClient.connect();
                        fKlineClient.connect();
                    }
                })
                .onDeactivate(new Runnable() {
                    public void run() {
                        log.info("[{}] STANDBY — Binance WebSocket 연결 해제", fInstanceId);
                        fTickerClient.disconnect();
                        fKlineClient.disconnect();
                    }
                })
                .build();

        // ── 종료 훅 ─────────────────────────────────────────────────────────
        final LeaderElector        fElector       = elector;
        final TickerEventHandler   fTickerHandler = tickerHandler;
        final KlineEventHandler    fKlineHandler  = klineHandler;
        final GenericKafkaProducer fProducer      = producer;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                log.info("[{}] Shutting down...", fInstanceId);
                fElector.shutdown();
                fTickerHandler.shutdown();
                fKlineHandler.shutdown();
                fProducer.close();
            }
        }));

        elector.start();
        Thread.currentThread().join();
    }

    private static Properties loadProperties(String path) throws Exception {
        Properties props = new Properties();
        var filePath = Paths.get(path);
        if (Files.exists(filePath)) {
            try (InputStream in = Files.newInputStream(filePath)) {
                props.load(in);
            }
        } else {
            try (InputStream in = Main.class.getClassLoader().getResourceAsStream(path)) {
                if (in == null) throw new IllegalArgumentException("Config not found: " + path);
                props.load(in);
            }
        }
        return props;
    }

    private static List<String> cap(List<String> symbols, int max, String label) {
        if (symbols.size() <= max) return symbols;
        log.warn("[{}] 심볼 {}개 → WS 한계 {}개로 제한", label, symbols.size(), max);
        return symbols.subList(0, max);
    }
}
