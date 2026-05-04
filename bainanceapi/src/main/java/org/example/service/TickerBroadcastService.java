package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.TickerDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka crypto-ticker 토픽을 구독하여 최신 시세를 캐시에 보관하고,
 * 0.5초마다 WebSocket 구독자 전체에 브로드캐스트하는 서비스.
 *
 * <p>기존 @Scheduled DB 폴링 방식 대비 변경점:
 *   - 데이터 소스: MariaDB 조회 → Kafka 메시지 (DB 우회)
 *   - 갱신 주기: 1초 타이머 → Kafka 이벤트 도착 즉시 캐시 업데이트
 *   - 브로드캐스트: 0.5초마다 캐시 전체를 /topic/tickers로 전송
 *
 * <p>Consumer Group: bainance-api-consumer-group (bainanceconsumer와 독립)
 */
@Service
public class TickerBroadcastService {

    private static final Logger log = LoggerFactory.getLogger(TickerBroadcastService.class);

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    /** symbol → 최신 TickerDto 캐시. Kafka 리스너와 스케줄러가 동시 접근하므로 ConcurrentHashMap 사용. */
    private final ConcurrentHashMap<String, TickerDto> cache = new ConcurrentHashMap<>();

    public TickerBroadcastService(SimpMessagingTemplate messagingTemplate, ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Kafka crypto-ticker 토픽에서 메시지를 수신하여 캐시를 갱신한다.
     * bainance-api-consumer-group은 bainanceconsumer(DB 저장)와 독립적으로 동작한다.
     */
    @KafkaListener(topics = "crypto-ticker", groupId = "bainance-api-consumer-group")
    public void onMessage(String message) {
        try {
            TickerDto ticker = objectMapper.readValue(message, TickerDto.class);
            cache.put(ticker.getSymbol(), ticker);
        } catch (Exception e) {
            log.error("Failed to parse ticker message: {}", e.getMessage());
        }
    }

    /**
     * 500ms마다 캐시에 있는 전체 심볼 시세를 WebSocket 구독자에게 브로드캐스트한다.
     * Kafka 이벤트가 없어도 주기적으로 실행되지만, 캐시가 비어있으면 전송하지 않는다.
     */
    @Scheduled(fixedDelay = 500)
    public void broadcast() {
        Collection<TickerDto> tickers = cache.values();
        if (tickers.isEmpty()) return;
        try {
            messagingTemplate.convertAndSend("/topic/tickers", tickers);
        } catch (Exception e) {
            log.error("Broadcast failed: {}", e.getMessage());
        }
    }
}
