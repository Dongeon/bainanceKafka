package org.example.kafka.consumer;

import java.io.IOException;

/**
 * KafkaMonitor를 자동으로 시작하는 애플리케이션 베이스 클래스.
 *
 * <p>개발자는 이 클래스를 상속하고 {@code run()}만 구현하면 된다.
 * HeartbeatMonitor, TopicMonitor는 생성자에서 자동으로 시작되며
 * 종료 시 ShutdownHook도 자동 등록된다.
 *
 * <p>기본 설정 파일: 실행 디렉토리의 {@code kafka.conf}
 * 경로를 변경하려면 {@code super("경로")} 를 사용한다.
 *
 * <pre>{@code
 *   public class Main extends KafkaApplication {
 *       public Main() throws IOException { super(); }
 *
 *       public void run() throws Exception {
 *           // 비즈니스 로직만 작성
 *       }
 *
 *       public static void main(String[] args) throws Exception {
 *           new Main().run();
 *       }
 *   }
 * }</pre>
 */
public abstract class KafkaApplication {

    private static final String DEFAULT_CONFIG = "kafka.conf";

    protected KafkaApplication() throws IOException {
        KafkaMonitor.fromConfig(DEFAULT_CONFIG).start();
    }

    protected KafkaApplication(String configPath) throws IOException {
        KafkaMonitor.fromConfig(configPath).start();
    }

    public abstract void run() throws Exception;
}
