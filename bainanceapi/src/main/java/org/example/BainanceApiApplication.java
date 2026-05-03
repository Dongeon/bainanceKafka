package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 바이낸스 모니터링 API 서버 진입점.
 *
 * <p>실행 방법:
 *   ./gradlew bootRun
 *   → http://localhost:8080
 *
 * <p>@EnableScheduling: TickerBroadcastService의 @Scheduled(1초마다 WebSocket 브로드캐스트)를 활성화한다.
 */
@SpringBootApplication
@EnableScheduling
public class BainanceApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(BainanceApiApplication.class, args);
    }
}
