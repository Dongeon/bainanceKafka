package org.example.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * STOMP over WebSocket 설정.
 *
 * <p>React 클라이언트 연결 방법:
 * <pre>
 *   const client = new Client({
 *     brokerURL: 'ws://localhost:8080/ws',
 *   });
 *   client.onConnect = () => {
 *     client.subscribe('/topic/tickers', (msg) => {
 *       const tickers = JSON.parse(msg.body);
 *       // 화면 업데이트
 *     });
 *   };
 *   client.activate();
 * </pre>
 *
 * <p>STOMP를 사용하는 이유:
 *   순수 WebSocket은 메시지 라우팅/구독 관리를 직접 구현해야 한다.
 *   STOMP는 토픽 기반 pub/sub 모델을 제공하므로 클라이언트 코드가 단순해진다.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // "/topic/**"으로 시작하는 목적지는 인메모리 브로커가 처리한다.
        // 서버 → 클라이언트 방향 브로드캐스트에 사용된다.
        registry.enableSimpleBroker("/topic");

        // 클라이언트 → 서버 메시지의 prefix. 현재는 서버로 보내는 메시지가 없어 사용 안 하지만,
        // 추후 기능 추가(예: 알림 구독)를 위해 설정해둔다.
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // React 클라이언트가 WebSocket 연결을 맺는 엔드포인트.
        // setAllowedOriginPatterns("*")는 개발 환경 CORS 허용이며, 운영 배포 시 도메인을 명시해야 한다.
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*");
    }
}
