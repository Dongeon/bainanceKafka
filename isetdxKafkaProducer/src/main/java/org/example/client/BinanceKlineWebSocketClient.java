package org.example.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okio.ByteString;
import org.example.handler.KlineEventHandler;
import org.example.model.KlineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 바이낸스 Kline(캔들스틱) WebSocket 클라이언트.
 *
 * <p>심볼 × 인터벌 조합을 Combined Stream 단일 연결로 구독한다.
 * 예: 10 심볼 × 4 인터벌 = 40 스트림 (한계 1024개 대비 여유 있음)
 *
 * <p>is_closed=true인 확정 캔들만 Kafka로 전송한다. (KlineEventHandler에서 필터링)
 */
public class BinanceKlineWebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(BinanceKlineWebSocketClient.class);
    private static final String BASE_URL = "wss://stream.binance.com:9443/stream?streams=";
    private static final int RECONNECT_DELAY_SEC = 5;
    private static final int MAX_RECONNECT_ATTEMPTS = 10;

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final KlineEventHandler eventHandler;
    private final List<String> symbols;
    private final List<String> intervals;

    private WebSocket webSocket;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private int reconnectAttempts = 0;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public BinanceKlineWebSocketClient(List<String> symbols, List<String> intervals,
                                       KlineEventHandler eventHandler) {
        this.symbols      = symbols;
        this.intervals    = intervals;
        this.eventHandler = eventHandler;
        this.objectMapper = new ObjectMapper();
        this.httpClient   = new OkHttpClient.Builder()
                .pingInterval(20, TimeUnit.SECONDS)
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS)
                .build();
    }

    public void connect() {
        running.set(true);
        reconnectAttempts = 0;
        openConnection();
    }

    public void disconnect() {
        running.set(false);
        scheduler.shutdownNow();
        if (webSocket != null) webSocket.close(1000, "Client disconnect");
        httpClient.dispatcher().executorService().shutdown();
    }

    private void openConnection() {
        String url = buildStreamUrl();
        log.info("Kline WebSocket connecting: {} streams", symbols.size() * intervals.size());
        log.debug("URL: {}", url);
        Request request = new Request.Builder().url(url).build();
        webSocket = httpClient.newWebSocket(request, new KlineWebSocketListener());
    }

    /** symbols × intervals 조합으로 Combined Stream URL을 생성한다. */
    private String buildStreamUrl() {
        List<String> streamList = new ArrayList<>();
        for (String s : symbols) {
            for (String ivl : intervals) {
                streamList.add(s.toLowerCase() + "@kline_" + ivl);
            }
        }
        return BASE_URL + String.join("/", streamList);
    }

    private void scheduleReconnect() {
        if (!running.get()) return;
        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            log.error("Kline: max reconnect attempts reached. Giving up.");
            return;
        }
        long delay = (long) RECONNECT_DELAY_SEC * (1L << Math.min(reconnectAttempts, 4));
        reconnectAttempts++;
        log.info("Kline: reconnecting in {}s (attempt {}/{})", delay, reconnectAttempts, MAX_RECONNECT_ATTEMPTS);
        scheduler.schedule(this::openConnection, delay, TimeUnit.SECONDS);
    }

    private class KlineWebSocketListener extends WebSocketListener {

        @Override
        public void onOpen(WebSocket ws, Response response) {
            log.info("Kline WebSocket connected. symbols={} intervals={}", symbols.size(), intervals);
            reconnectAttempts = 0;
        }

        @Override
        public void onMessage(WebSocket ws, String text) {
            try {
                var node     = objectMapper.readTree(text);
                var dataNode = node.get("data");
                if (dataNode == null) return;

                KlineEvent event = objectMapper.treeToValue(dataNode, KlineEvent.class);
                eventHandler.handle(event);

            } catch (Exception e) {
                log.error("Kline parse error: {}", e.getMessage());
            }
        }

        @Override
        public void onMessage(WebSocket ws, ByteString bytes) { onMessage(ws, bytes.utf8()); }

        @Override
        public void onFailure(WebSocket ws, Throwable t, Response response) {
            log.error("Kline WebSocket error: {}", t.getMessage());
            scheduleReconnect();
        }

        @Override
        public void onClosing(WebSocket ws, int code, String reason) {
            log.warn("Kline WebSocket closing: code={}", code);
            ws.close(1000, null);
        }

        @Override
        public void onClosed(WebSocket ws, int code, String reason) {
            log.info("Kline WebSocket closed: code={}", code);
            if (running.get() && code != 1000) scheduleReconnect();
        }
    }
}
