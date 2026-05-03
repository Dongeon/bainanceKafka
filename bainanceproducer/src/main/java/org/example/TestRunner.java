package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.example.client.BinanceWebSocketClient;
import org.example.handler.TickerEventHandler;
import org.example.kafka.KafkaTickerProducer;
import org.example.model.TickerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka 전송 없이 WebSocket 수신 데이터를 콘솔로 확인하는 개발/디버깅용 테스트 러너.
 *
 * <p>용도:
 *   - Kafka 없이 바이낸스 WebSocket 연결 및 데이터 파싱이 정상인지 확인
 *   - TickerEvent JSON 구조와 파티션 매핑 결과를 눈으로 검증
 *   - 실제 운영 환경(Main)을 실행하기 전 사전 확인
 *
 * <p>실행 방법:
 *   IDE에서 TestRunner를 직접 실행하거나, build.gradle의 mainClass를
 *   "org.example.TestRunner"로 바꿔 ./gradlew run 으로 실행한다.
 *   MAX_MESSAGES 개 메시지 수신 후 자동 종료되므로 Ctrl+C 불필요.
 *
 * <p>주의:
 *   이 클래스는 테스트 전용이며, 실제 Kafka Producer는 사용하지 않는다.
 *   KafkaTickerProducer를 익명 클래스로 오버라이드하여 Kafka 전송 대신 콘솔 출력만 수행한다.
 */
public class TestRunner {

    private static final Logger log = LoggerFactory.getLogger(TestRunner.class);

    /** 수신 후 자동 종료할 메시지 수. 검증에 충분한 수로 설정. */
    private static final int MAX_MESSAGES = 20;

    /**
     * murmur2 파티션 시뮬레이션에 사용할 파티션 수.
     * 실제 Kafka 토픽의 파티션 수(10)와 동일하게 맞춰야 정확한 파티션 번호가 출력된다.
     */
    private static final int NUM_PARTITIONS = 10;

    public static void main(String[] args) throws InterruptedException {
        List<String> symbols = List.of(
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
                "XRPUSDT", "DOTUSDT", "AVAXUSDT", "ATOMUSDT", "NEARUSDT"
        );

        // 콘솔 출력용 들여쓰기 적용 ObjectMapper
        ObjectMapper prettyMapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT);

        // MAX_MESSAGES 개 메시지 수신 시 latch.countDown()이 0이 되어 main 스레드가 깨어난다.
        CountDownLatch latch = new CountDownLatch(MAX_MESSAGES);
        AtomicInteger count = new AtomicInteger(0);

        // KafkaTickerProducer를 익명 클래스로 오버라이드.
        // send()에서 Kafka 전송 대신 ProducerRecord 시뮬레이션 결과를 콘솔에 출력한다.
        TickerEventHandler handler = new TickerEventHandler(new KafkaTickerProducer("localhost:9092") {
            @Override
            public void send(TickerEvent event) {
                int seq = count.incrementAndGet();
                // Kafka DefaultPartitioner가 사용하는 murmur2 알고리즘으로 파티션 번호를 계산한다.
                int simulatedPartition = simulatePartition(event.getSymbol(), NUM_PARTITIONS);

                try {
                    String json = prettyMapper.writeValueAsString(event);
                    System.out.println();
                    System.out.println("┌─────────────────────────────────────────────");
                    System.out.printf ("│ [%02d/%d] ProducerRecord%n", seq, MAX_MESSAGES);
                    System.out.println("├─────────────────────────────────────────────");
                    System.out.printf ("│  topic     : %s%n", KafkaTickerProducer.TOPIC);
                    System.out.printf ("│  key       : %s  (partition key)%n", event.getSymbol());
                    System.out.printf ("│  partition : %d  (murmur2(\"%s\") %% %d)%n",
                            simulatedPartition, event.getSymbol(), NUM_PARTITIONS);
                    System.out.println("│  value     :");
                    for (String line : json.split("\n")) {
                        System.out.println("│    " + line);
                    }
                    System.out.println("└─────────────────────────────────────────────");
                } catch (Exception e) {
                    log.error("Serialization error", e);
                }

                latch.countDown();
            }

            // TestRunner는 Kafka에 실제 연결하지 않으므로 close는 아무것도 하지 않는다.
            @Override
            public void close() { /* no-op */ }
        });

        BinanceWebSocketClient client = new BinanceWebSocketClient(symbols, handler);
        client.connect();

        System.out.println("=== Binance WebSocket 연결 대기 중... ===");
        System.out.printf("=== %d개 메시지 수신 후 자동 종료 ===%n%n", MAX_MESSAGES);

        // MAX_MESSAGES 개 수신 완료까지 대기
        latch.await();
        client.disconnect();
        System.out.println("\n=== 테스트 완료 ===");
    }

    /**
     * Kafka DefaultPartitioner의 파티션 계산 로직을 시뮬레이션한다.
     *
     * <p>실제 Kafka는 키를 murmur2 해시한 뒤 파티션 수로 나머지 연산을 한다.
     * 이 메서드는 동일한 알고리즘을 구현하여 실제 Kafka 전송 시의 파티션 번호를 미리 예측한다.
     *
     * @param key           파티션 키 (심볼 문자열, 예: "BTCUSDT")
     * @param numPartitions Kafka 토픽의 파티션 수
     * @return 0 이상 numPartitions 미만의 파티션 번호
     */
    private static int simulatePartition(String key, int numPartitions) {
        byte[] bytes = key.getBytes();
        int hash = murmur2(bytes);
        // Integer.MAX_VALUE로 AND 연산하여 음수 해시값을 양수로 만든 뒤 나머지 연산
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }

    /**
     * MurmurHash2 알고리즘 구현체.
     *
     * <p>Kafka DefaultPartitioner가 사용하는 것과 동일한 구현이다.
     * 참고: org.apache.kafka.common.utils.Utils#murmur2
     *
     * @param data 해시할 바이트 배열
     * @return 32비트 정수 해시값 (음수일 수 있음)
     */
    private static int murmur2(byte[] data) {
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ data.length;
        int i = 0;
        // 4바이트씩 처리
        while (i + 4 <= data.length) {
            int k = (data[i] & 0xFF) | ((data[i+1] & 0xFF) << 8)
                  | ((data[i+2] & 0xFF) << 16) | ((data[i+3] & 0xFF) << 24);
            k *= m; k ^= k >>> r; k *= m;
            h *= m; h ^= k;
            i += 4;
        }
        // 나머지 1~3바이트 처리 (fall-through 의도적)
        switch (data.length - i) {
            case 3: h ^= (data[i+2] & 0xFF) << 16;
            case 2: h ^= (data[i+1] & 0xFF) << 8;
            case 1: h ^= (data[i] & 0xFF); h *= m;
        }
        // 최종 믹싱
        h ^= h >>> 13; h *= m; h ^= h >>> 15;
        return h;
    }
}
