# bainanceconsumer 아키텍처

## 1. 모듈 책임

Kafka에서 ticker와 kline 메시지를 소비하여 MariaDB에 저장한다. 이 모듈은 **쓰기 전용**이며 API를 제공하지 않는다.

---

## 2. 컴포넌트 구성

```
Main
├── TickerConsumer  ← crypto-ticker 토픽 소비, 메인 스레드에서 블로킹 실행
├── KlineConsumer   ← crypto-kline  토픽 소비, 별도 "kline-consumer" 스레드
├── TickerRepository ← HikariCP + JDBC INSERT (ticker 테이블)
└── KlineRepository  ← HikariCP + JDBC INSERT ... ON DUPLICATE KEY UPDATE (kline 테이블)
```

---

## 3. 핵심 설계 결정

### 3-1. 멀티스레드 구조: 메인 스레드 + 별도 스레드
```java
Thread klineThread = new Thread(klineConsumer, "kline-consumer");
klineThread.start();
tickerConsumer.run();  // 메인 스레드에서 블로킹
```
두 Consumer는 서로 다른 Kafka 토픽(`crypto-ticker`, `crypto-kline`)을 구독한다. 하나가 블로킹되어도 다른 쪽에 영향이 없다. `Thread klineThread`에 이름 `"kline-consumer"`를 붙여 스레드 덤프에서 식별 가능하게 함.

왜 ExecutorService가 아닌 Thread를 직접 쓰는가? 스레드가 딱 2개이고, Consumer는 무한 루프(`while (running)`)로 동작하므로 스레드 풀의 이점이 없다.

### 3-2. 수동 커밋 (ENABLE_AUTO_COMMIT=false)
```java
// 소비 → DB 저장 완료 → 오프셋 커밋
consumer.poll(Duration.ofMillis(1000));
// ... DB save ...
consumer.commitSync();
```
자동 커밋은 DB 저장 전에 오프셋을 커밋할 수 있다. 장애 발생 시 데이터 소실. 수동 커밋은 "저장 성공 확인 후 커밋"을 보장한다. `commitSync()`는 브로커 확인까지 블로킹하므로 신뢰도가 높다 (처리량보다 정확성 우선).

### 3-3. ON DUPLICATE KEY UPDATE (upsert)
```sql
INSERT INTO kline (symbol, interval_type, open_time, ...)
VALUES (?, ?, ?, ...)
ON DUPLICATE KEY UPDATE
    open_price = VALUES(open_price), ...
```
Kafka는 at-least-once 전달을 보장한다. Consumer 재시작 시 마지막 처리 메시지가 중복 전달될 수 있다. UNIQUE KEY `(symbol, interval_type, open_time)`가 충돌하면 INSERT가 아닌 UPDATE로 처리 → 멱등성 보장.

Ticker는 매 건이 고유한 시점 데이터이므로 단순 INSERT.

### 3-4. HikariCP 설정
```java
config.setMaximumPoolSize(5);    // Consumer 스레드 2개 + 여유
config.setMinimumIdle(2);        // 유휴 시에도 연결 2개 유지 (재연결 비용 절감)
config.setConnectionTimeout(3000); // 3초 안에 커넥션 못 얻으면 예외
config.setPoolName("kline-pool");  // JMX/로그에서 식별용
```
Consumer 스레드가 2개이므로 maxPool=5면 충분하다. 데이터 피크 시에도 각 스레드가 1개씩 사용하고 나머지 3개는 여유분.

### 3-5. BigDecimal 타입 변환
```java
ps.setBigDecimal(5, new BigDecimal(e.getOpenPrice()));
```
Binance가 전송하는 가격은 문자열(`"50000.12345678"`). `double`로 변환하면 부동소수점 오차 발생. `BigDecimal`로 정확한 소수점 저장.

---

## 4. 변수명 규칙

| 패턴 | 예시 | 이유 |
|------|------|------|
| `Repo` 접미사 | `TickerRepository`, `KlineRepository` | DB 접근 객체임을 명시 |
| `Consumer` 접미사 | `TickerConsumer`, `KlineConsumer` | Kafka 소비자임을 명시 |
| SQL 상수 | `INSERT_SQL` (static final) | SQL 문자열을 한 곳에 모아 관리 |
| KST 명시 | `openTimeKst`, `closeTimeKst` | 타임존 혼동 방지 |

---

## 5. 오류 처리 전략

| 상황 | 처리 방법 |
|------|----------|
| DB 저장 실패 | `log.error` 후 다음 메시지 계속 처리 (데이터 소실 감수, 서비스 중단 방지) |
| JSON 역직렬화 실패 | `log.error` 후 skip (손상된 메시지가 Consumer를 죽이지 않음) |
| Kafka 브로커 연결 끊김 | `KafkaConsumer.poll()` 내부에서 재연결 시도 (Kafka 클라이언트 기본 동작) |
| Shutdown Hook | `consumer.close()` → `commitSync()` 자동 호출, `dataSource.close()` |

---

## 6. 개선 여지

1. **설정 외부화**: `DB_URL`, `DB_PASSWORD`가 소스 코드에 평문으로 노출 → `config.properties` 또는 환경변수 권장. 현재는 개발/학습 목적으로 허용
2. **Consumer Group 독립화**: 두 Consumer가 다른 그룹 ID(`bainance-consumer-group`, `bainance-kline-consumer-group`)를 사용 중. 이미 올바른 설정 (같은 그룹이면 파티션 경쟁 발생)
3. **재처리 정책**: DB 저장 실패 시 현재는 로그만 남김. Dead Letter Queue(DLQ) 토픽을 만들어 실패 메시지를 보관하면 사후 재처리 가능
4. **Kline Repository 패키지 위치**: `service` 패키지에 `KlineRepository`, `TickerRepository`가 있는데, 실제로는 리포지토리 패턴 구현체이므로 `repository` 패키지가 더 정확함
