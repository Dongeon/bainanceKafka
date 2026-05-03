package org.example.handler;

import org.example.kafka.KafkaTickerProducer;
import org.example.model.TickerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WebSocket에서 수신한 TickerEvent를 Kafka로 전달하는 핸들러.
 *
 * <p>역할:
 *   BinanceWebSocketClient가 파싱 완료한 TickerEvent를 받아
 *   KafkaTickerProducer를 통해 Kafka 토픽으로 전송한다.
 *   또한 60초마다 누적 전송 건수를 로그로 출력하여 시스템이 정상 동작 중임을 알린다.
 *
 * <p>데이터 흐름에서의 위치:
 *   BinanceWebSocketClient → (이 클래스) → KafkaTickerProducer → Kafka
 */
public class TickerEventHandler {

    private static final Logger log = LoggerFactory.getLogger(TickerEventHandler.class);

    /** 상태 로그를 출력하는 주기(초). 너무 잦으면 로그가 넘치므로 60초로 설정. */
    private static final int STATUS_INTERVAL_SEC = 60;

    /** 실제 Kafka 전송을 담당하는 프로듀서 */
    private final KafkaTickerProducer kafkaProducer;

    /**
     * 애플리케이션 시작 이후 Kafka로 전송된 메시지 총 건수.
     * handle()은 OkHttp 내부 스레드에서 호출되므로 스레드 안전한 AtomicLong을 사용한다.
     */
    private final AtomicLong messageCount = new AtomicLong(0);

    /** 주기적 상태 로그를 실행하는 단일 스레드 스케줄러 */
    private final ScheduledExecutorService statusScheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * 생성자. KafkaTickerProducer를 주입받고 상태 로거를 시작한다.
     *
     * @param kafkaProducer Kafka로 이벤트를 전송할 프로듀서
     */
    public TickerEventHandler(KafkaTickerProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        startStatusLogger();
    }

    /**
     * TickerEvent를 수신하여 Kafka로 전송한다.
     * BinanceWebSocketClient의 onMessage 콜백에서 파싱 직후 호출된다.
     *
     * @param event 바이낸스로부터 수신한 시세 이벤트
     */
    public void handle(TickerEvent event) {
        kafkaProducer.send(event);
        messageCount.incrementAndGet();
    }

    /**
     * 60초마다 누적 전송 건수를 로그로 출력하는 스케줄러를 시작한다.
     *
     * <p>이 로그는 "프로세스가 살아있고 데이터가 정상적으로 흐르고 있다"는 것을 확인하기 위한 헬스체크용이다.
     * 60초 동안 이 로그가 출력되지 않으면 연결 문제나 프로세스 중단을 의심해야 한다.
     */
    private void startStatusLogger() {
        statusScheduler.scheduleAtFixedRate(() ->
            log.info("[STATUS] 정상 수신 중 — 누적 전송 {}건", messageCount.get()),
            STATUS_INTERVAL_SEC, STATUS_INTERVAL_SEC, TimeUnit.SECONDS
        );
    }

    /**
     * 상태 로거 스케줄러를 종료한다.
     * Main의 ShutdownHook에서 호출되며, 반드시 호출해야 JVM이 정상 종료된다.
     */
    public void shutdown() {
        statusScheduler.shutdownNow();
    }
}
