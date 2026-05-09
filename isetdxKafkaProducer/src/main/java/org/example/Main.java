package org.example;

import org.example.client.BinanceKlineWebSocketClient;
import org.example.client.BinanceWebSocketClient;
import org.example.handler.KlineEventHandler;
import org.example.handler.TickerEventHandler;
import org.example.kafka.leader.LeaderElector;
import org.example.kafka.leader.LeaderSettings;
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

    private static final String DEFAULT_CONFIG = "kafka.conf";

    private static final List<String> SYMBOLS = Arrays.asList(
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
            "XRPUSDT", "DOTUSDT", "AVAXUSDT", "ATOMUSDT", "NEARUSDT"
    );
    private static final List<String> KLINE_INTERVALS = Arrays.asList("1m", "5m", "15m", "1h");

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG;

        Properties conf = loadProperties(configPath);

        if (args.length >= 2 && !args[1].isBlank()) conf.setProperty("leader.instance.id",     args[1]);
        if (args.length >= 3 && !args[2].isBlank()) conf.setProperty("leader.instance.weight", args[2]);

        String instanceId = conf.getProperty("leader.instance.id", "producer-?");
        log.info("Starting isetdxKafkaProducer: config={}, instanceId={}, symbols={}, weight={}",
                configPath, instanceId, SYMBOLS.size(), conf.getProperty("leader.instance.weight", "100"));

        String tickerTopic = conf.getProperty("binance.ticker.topic", "crypto-ticker");
        String klineTopic  = conf.getProperty("binance.kline.topic",  "crypto-kline");

        List<String> tickerSymbols = SYMBOLS;
        List<String> klineSymbols  = SYMBOLS;

        // ── IsetDx_kafka-producer-lib ────────────────────────────────────────
        GenericKafkaProducer producer = GenericKafkaProducer.fromConfig(configPath);

        // ── 핸들러 ───────────────────────────────────────────────────────────
        TickerEventHandler tickerHandler = new TickerEventHandler(producer, tickerTopic);
        KlineEventHandler  klineHandler  = new KlineEventHandler(producer,  klineTopic);

        // ── WebSocket 클라이언트 (ACTIVE 전환 시에만 connect) ─────────────────
        BinanceWebSocketClient      tickerClient = new BinanceWebSocketClient(tickerSymbols, tickerHandler);
        BinanceKlineWebSocketClient klineClient  = new BinanceKlineWebSocketClient(klineSymbols, KLINE_INTERVALS, klineHandler);

        // ── IsetDx_kafka-leader-lib ──────────────────────────────────────────
        final BinanceWebSocketClient      fTickerClient  = tickerClient;
        final BinanceKlineWebSocketClient fKlineClient   = klineClient;
        final String                      fInstanceId    = instanceId;

        LeaderElector elector = LeaderElector.fromSettings(LeaderSettings.fromProperties(conf))
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

}
