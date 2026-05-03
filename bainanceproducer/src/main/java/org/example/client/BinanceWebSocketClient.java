package org.example.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okio.ByteString;
import org.example.handler.TickerEventHandler;
import org.example.model.TickerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 바이낸스(Binance) WebSocket 클라이언트
 *
 * <p>역할:
 *   바이낸스의 Combined Stream API에 WebSocket으로 연결하여
 *   여러 암호화폐 심볼의 실시간 시세(ticker) 데이터를 동시에 수신한다.
 *   수신한 데이터는 TickerEventHandler를 통해 Kafka로 전송된다.
 *
 * <p>전체 데이터 흐름:
 *   Binance WebSocket → (이 클래스) → TickerEventHandler → Kafka → Consumer → MariaDB
 *
 * <p>주요 특징:
 *   - Combined Stream 단일 연결로 최대 1024개 심볼 동시 구독 가능 (현재 10개 사용)
 *   - 연결 끊김 시 Exponential Backoff 방식으로 자동 재연결 (최대 10회 시도)
 *   - OkHttp의 ping/pong으로 20초마다 연결 상태를 확인하여 좀비 연결 방지
 */
public class BinanceWebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketClient.class);

    /**
     * 바이낸스 Combined Stream 엔드포인트.
     * 이 URL 뒤에 "btcusdt@ticker/ethusdt@ticker/..." 형식으로 스트림 이름을 붙인다.
     *
     * 참고: https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams
     */
    private static final String BASE_URL = "wss://stream.binance.com:9443/stream?streams=";

    /** 재연결 시 기본 대기 시간(초). Exponential Backoff의 기준값으로 사용된다. */
    private static final int RECONNECT_DELAY_SEC = 5;

    /**
     * 최대 재연결 시도 횟수.
     * 이 횟수를 초과하면 재연결을 포기하고 에러 로그를 남긴 뒤 종료한다.
     */
    private static final int MAX_RECONNECT_ATTEMPTS = 10;

    /** HTTP/WebSocket 연결을 담당하는 OkHttp 클라이언트 */
    private final OkHttpClient httpClient;

    /** JSON 파싱에 사용하는 Jackson ObjectMapper */
    private final ObjectMapper objectMapper;

    /**
     * 수신한 TickerEvent를 Kafka로 전송하는 핸들러.
     * 실제 Kafka 전송 로직은 이 핸들러 내부에 구현되어 있다.
     */
    private final TickerEventHandler eventHandler;

    /**
     * 구독할 심볼 목록 (대문자 형식).
     * 예: ["BTCUSDT", "ETHUSDT", "BNBUSDT", ...]
     * Main 클래스에서 주입되며, URL 생성 시 소문자로 변환된다.
     */
    private final List<String> symbols;

    /** 현재 활성화된 WebSocket 연결 객체. 연결 전 또는 종료 후에는 null일 수 있다. */
    private WebSocket webSocket;

    /**
     * 클라이언트 실행 상태 플래그.
     * true: 정상 운영 중 (재연결 허용)
     * false: 의도적으로 종료된 상태 (재연결 시도 안 함)
     *
     * 멀티스레드 환경(OkHttp 내부 스레드 + 재연결 스케줄러)에서 안전하게 읽고 쓰기 위해
     * AtomicBoolean을 사용한다.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** 현재까지 재연결을 시도한 횟수. 연결 성공 시 0으로 초기화된다. */
    private int reconnectAttempts = 0;

    /**
     * 재연결 딜레이 스케줄링에 사용하는 단일 스레드 스케줄러.
     * disconnect() 호출 시 shutdownNow()로 즉시 종료된다.
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * 생성자.
     *
     * @param symbols     구독할 심볼 목록 (예: ["BTCUSDT", "ETHUSDT"])
     * @param eventHandler 수신한 이벤트를 Kafka로 전달하는 핸들러
     */
    public BinanceWebSocketClient(List<String> symbols, TickerEventHandler eventHandler) {
        this.symbols = symbols;
        this.eventHandler = eventHandler;
        this.objectMapper = new ObjectMapper();
        this.httpClient = new OkHttpClient.Builder()
                // 20초마다 ping 프레임을 보내 서버가 살아있는지 확인.
                // 바이낸스 서버는 응답이 없으면 연결을 끊으므로 이 설정이 필수다.
                .pingInterval(20, TimeUnit.SECONDS)
                .connectTimeout(10, TimeUnit.SECONDS)
                // WebSocket은 데이터가 올 때까지 무한히 대기해야 하므로 readTimeout을 0(무제한)으로 설정.
                // 일반 HTTP 요청에서는 절대 0으로 설정하면 안 된다.
                .readTimeout(0, TimeUnit.SECONDS)
                .build();
    }

    /**
     * WebSocket 연결을 시작한다.
     * 앱 시작 시 Main 클래스에서 최초 1회 호출된다.
     */
    public void connect() {
        running.set(true);
        reconnectAttempts = 0;
        openConnection();
    }

    /**
     * WebSocket 연결을 정상 종료한다.
     * 종료 후에는 재연결이 시도되지 않는다.
     * 애플리케이션 종료 시 반드시 호출해야 OkHttp 내부 스레드가 정리된다.
     */
    public void disconnect() {
        running.set(false);
        scheduler.shutdownNow();
        if (webSocket != null) {
            // code 1000 = 정상 종료 (RFC 6455 표준)
            webSocket.close(1000, "Client disconnect");
        }
        // OkHttp가 내부적으로 유지하는 커넥션 풀 스레드도 종료
        httpClient.dispatcher().executorService().shutdown();
    }

    /**
     * 실제로 WebSocket 연결을 여는 내부 메서드.
     * connect() 최초 호출과 재연결 스케줄러 모두 이 메서드를 사용한다.
     */
    private void openConnection() {
        String streamUrl = buildStreamUrl();
        log.info("Connecting to: {}", streamUrl);

        Request request = new Request.Builder().url(streamUrl).build();
        // 연결 및 이후 모든 이벤트(onOpen, onMessage, onFailure 등)는
        // OkHttp 내부 스레드에서 BinanceWebSocketListener가 비동기로 처리한다.
        webSocket = httpClient.newWebSocket(request, new BinanceWebSocketListener());
    }

    /**
     * 심볼 목록을 바이낸스 Combined Stream URL 형식으로 변환한다.
     *
     * <p>변환 예시:
     *   입력: ["BTCUSDT", "ETHUSDT"]
     *   출력: "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker"
     *
     * <p>@ticker는 개별 심볼의 24시간 롤링 통계(현재가, 변동률, 거래량 등)를 제공하는 스트림 타입이다.
     */
    private String buildStreamUrl() {
        String streams = symbols.stream()
                .map(s -> s.toLowerCase() + "@ticker")
                .collect(Collectors.joining("/"));
        return BASE_URL + streams;
    }

    /**
     * 재연결을 스케줄링한다.
     *
     * <p>Exponential Backoff 전략:
     *   대기 시간 = RECONNECT_DELAY_SEC × 2^(시도 횟수, 최대 4승)
     *   - 1회차: 5 × 1  =  5초
     *   - 2회차: 5 × 2  = 10초
     *   - 3회차: 5 × 4  = 20초
     *   - 4회차: 5 × 8  = 40초
     *   - 5회차 이후: 5 × 16 = 80초 (상한선)
     *
     * <p>이 전략으로 순간적인 네트워크 장애 시 빠르게 재연결을 시도하면서,
     *   서버 과부하는 방지할 수 있다.
     */
    private void scheduleReconnect() {
        if (!running.get()) return; // 의도적 종료 시에는 재연결하지 않음

        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            log.error("Max reconnect attempts ({}) reached. Giving up.", MAX_RECONNECT_ATTEMPTS);
            return;
        }

        // Math.min(reconnectAttempts, 4)로 지수를 최대 4(= 16배)로 제한
        long delay = (long) RECONNECT_DELAY_SEC * (1L << Math.min(reconnectAttempts, 4));
        reconnectAttempts++;
        log.info("Reconnecting in {}s (attempt {}/{})", delay, reconnectAttempts, MAX_RECONNECT_ATTEMPTS);
        scheduler.schedule(this::openConnection, delay, TimeUnit.SECONDS);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // WebSocket 이벤트 리스너
    //
    // OkHttp의 WebSocketListener를 구현한 내부 클래스.
    // 연결, 메시지 수신, 오류, 연결 종료 등 WebSocket 생명주기 이벤트를 처리한다.
    // 모든 콜백은 OkHttp 내부 스레드에서 호출된다.
    // ────────────────────────────────────────────────────────────────────────────
    private class BinanceWebSocketListener extends WebSocketListener {

        /**
         * WebSocket 연결이 성공적으로 열렸을 때 호출된다.
         * 연결 성공 시 재연결 카운터를 초기화하여 다음 장애 시 처음부터 다시 시도한다.
         */
        @Override
        public void onOpen(WebSocket ws, Response response) {
            log.info("WebSocket connected. Subscribed symbols: {}", symbols);
            reconnectAttempts = 0;
        }

        /**
         * 서버로부터 텍스트 메시지(JSON)를 수신했을 때 호출된다.
         *
         * <p>바이낸스 Combined Stream의 메시지 구조:
         * <pre>
         * {
         *   "stream": "btcusdt@ticker",
         *   "data": {
         *     "e": "24hrTicker",  // 이벤트 타입
         *     "s": "BTCUSDT",     // 심볼
         *     "c": "62000.00",    // 현재가
         *     ...                 // TickerEvent 모델의 나머지 필드들
         *   }
         * }
         * </pre>
         *
         * "stream" 필드는 어떤 심볼의 데이터인지 식별용이므로 무시하고,
         * "data" 필드만 꺼내 TickerEvent로 역직렬화한다.
         */
        @Override
        public void onMessage(WebSocket ws, String text) {
            try {
                var node = objectMapper.readTree(text);
                var dataNode = node.get("data");
                // "data" 필드가 없는 메시지(예: 구독 확인 응답)는 무시
                if (dataNode == null) return;

                TickerEvent event = objectMapper.treeToValue(dataNode, TickerEvent.class);
                // 파싱 완료 후 Kafka 전송 핸들러에 위임
                eventHandler.handle(event);

            } catch (Exception e) {
                // 파싱 실패해도 프로세스를 죽이지 않고 로그만 남기고 다음 메시지 처리 계속
                log.error("Failed to parse message: {}", text, e);
            }
        }

        /**
         * 바이너리 메시지 수신 시 호출된다.
         * 바이낸스는 텍스트(JSON)만 전송하지만, 혹시 모를 바이너리 프레임도 처리하기 위해
         * UTF-8로 디코딩 후 텍스트 메시지 핸들러에 위임한다.
         */
        @Override
        public void onMessage(WebSocket ws, ByteString bytes) {
            onMessage(ws, bytes.utf8());
        }

        /**
         * 네트워크 오류, 타임아웃 등 예외 상황으로 연결이 끊겼을 때 호출된다.
         * onClosed()와 달리 정상 종료 코드 없이 비정상 종료된 경우다.
         * 재연결을 스케줄링하여 서비스 연속성을 유지한다.
         */
        @Override
        public void onFailure(WebSocket ws, Throwable t, Response response) {
            log.error("WebSocket error: {}", t.getMessage());
            scheduleReconnect();
        }

        /**
         * 서버가 연결 종료를 요청했을 때 호출된다(서버 → 클라이언트 Close 프레임).
         * OkHttp 명세에 따라 클라이언트도 Close 프레임을 응답해야 한다.
         * 이후 onClosed()가 호출되면서 재연결 여부를 판단한다.
         */
        @Override
        public void onClosing(WebSocket ws, int code, String reason) {
            log.warn("WebSocket closing: code={} reason={}", code, reason);
            ws.close(1000, null);
        }

        /**
         * 양측 모두 Close 프레임 교환이 완료되어 연결이 완전히 닫혔을 때 호출된다.
         *
         * <p>재연결 정책:
         *   - running == false: disconnect()로 의도적 종료 → 재연결 안 함
         *   - code == 1000: 정상 종료(서버 재시작 등) → 재연결 안 함
         *   - 그 외: 예기치 않은 종료 → 재연결 시도
         *
         * 바이낸스는 24시간마다 연결을 끊으므로 (code != 1000 케이스),
         * 이 재연결 로직이 실제로 자주 동작한다.
         */
        @Override
        public void onClosed(WebSocket ws, int code, String reason) {
            log.info("WebSocket closed: code={} reason={}", code, reason);
            if (running.get() && code != 1000) {
                scheduleReconnect();
            }
        }
    }
}
