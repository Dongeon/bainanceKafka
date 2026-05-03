package org.example.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 1초마다 최신 시세를 WebSocket 구독자에게 브로드캐스트하는 서비스.
 *
 * <p>동작 방식:
 *   1. DB에서 10개 심볼 각각의 최신 레코드를 조회
 *   2. STOMP 토픽 "/topic/tickers"로 JSON 배열을 전송
 *   3. React 클라이언트가 해당 토픽을 구독하여 실시간으로 화면 업데이트
 *
 * <p>@Scheduled(fixedDelay = 1000):
 *   이전 실행이 완료된 후 1초 뒤에 다음 실행을 시작한다.
 *   fixedRate와 달리 DB 조회가 1초를 넘더라도 중복 실행되지 않는다.
 */
@Service
public class TickerBroadcastService {

    private static final Logger log = LoggerFactory.getLogger(TickerBroadcastService.class);

    /** STOMP 메시지를 토픽으로 전송하는 Spring 내장 템플릿 */
    private final SimpMessagingTemplate messagingTemplate;
    private final TickerService tickerService;

    public TickerBroadcastService(SimpMessagingTemplate messagingTemplate,
                                  TickerService tickerService) {
        this.messagingTemplate = messagingTemplate;
        this.tickerService = tickerService;
    }

    /**
     * 최신 시세를 WebSocket 구독자 전체에게 브로드캐스트한다.
     * BainanceApiApplication의 @EnableScheduling에 의해 1초마다 자동 호출된다.
     */
    @Scheduled(fixedDelay = 1000)
    public void broadcast() {
        try {
            var tickers = tickerService.getLatestAll();
            if (!tickers.isEmpty()) {
                // "/topic/tickers"를 구독한 모든 WebSocket 클라이언트에게 전송
                messagingTemplate.convertAndSend("/topic/tickers", tickers);
            }
        } catch (Exception e) {
            // 일시적인 DB 오류 등으로 브로드캐스트가 실패해도 스케줄러가 멈추지 않도록 예외를 잡는다
            log.error("Broadcast failed: {}", e.getMessage());
        }
    }
}
