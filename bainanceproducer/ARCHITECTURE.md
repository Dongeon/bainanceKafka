# bainanceproducer 아키텍처

## 1. 모듈 책임

바이낸스 WebSocket Combined Stream에 연결하여 실시간 시세(ticker)와 확정 캔들(kline)을 수신한 뒤 Kafka로 전송한다. 이 모듈은 **데이터 수집 전용**이며 DB를 직접 접근하지 않는다.

---

## 2. 컴포넌트 구성

```
Main
├── BinanceWebSocketClient      ← ticker 스트림 WS 연결
│   └── BinanceWebSocketListener (inner class) ← OkHttp 콜백
├── BinanceKlineWebSocketClient ← kline 스트림 WS 연결
├── TickerEventHandler          ← 필터링 없이 전량 Kafka 전송 + 통계 로깅
├── KlineEventHandler           ← is_closed=true 필터링 후 Kafka 전송 + 통계 로깅
├── KafkaTickerProducer         ← crypto-ticker 토픽
└── KafkaKlineProducer          ← crypto-kline 토픽
```

의존 방향: `Handler → KafkaProducer`, `Client → Handler`. Main이 전체를 조립하는 Composition Root 역할.

---

## 3. 핵심 설계 결정

### 3-1. OkHttp WebSocket 선택
Java 표준 라이브러리에는 WebSocket 클라이언트가 없다. 선택지:
- **OkHttp**: ping/pong 자동 처리, 비동기 콜백, Android/서버 모두 검증된 라이브러리
- Java-WebSocket (TooTallNate): 더 단순하지만 ping 관리를 직접 해야 함
- Netty: 과도한 복잡성

OkHttp를 선택한 이유:
- `pingInterval(20, SECONDS)` 한 줄로 바이낸스 연결 유지 처리
- `readTimeout(0)` — WebSocket은 데이터가 올 때까지 무한 대기해야 하므로 필수 설정
- `dispatcher().executorService().shutdown()` 으로 shutdown 시 스레드 정리 보장

### 3-2. Exponential Backoff 재연결
```java
long delay = RECONNECT_DELAY_SEC * (1L << Math.min(attempts, 4));
// 시도 1: 5s, 2: 10s, 3: 20s, 4: 40s, 5+: 80s (상한)
```
- 순간적 네트워크 장애 → 빠른 재시도(5초)
- 서버 과부하 상태 → 점진적 대기(최대 80초)
- 바이낸스는 연결을 24시간마다 강제 종료하므로 이 로직이 실제로 매일 동작

### 3-3. AtomicBoolean running 플래그
`disconnect()` 호출(메인 스레드)과 `onFailure()` 콜백(OkHttp 내부 스레드)이 동시에 `running`을 읽을 수 있다. `AtomicBoolean`으로 CAS(Compare-And-Swap) 없이도 가시성 보장.

### 3-4. KlineEvent의 WRITE_ONLY + rawSymbol 패턴
```java
// 잘못된 방법 (이전 버그):
@JsonProperty(value = "s", access = WRITE_ONLY)
private String symbol;          // 필드명 = symbol
public String getSymbol() {...} // Jackson이 getter를 WRITE_ONLY 취급 → 직렬화 누락!

// 올바른 방법:
@JsonProperty(value = "s", access = WRITE_ONLY)
private String rawSymbol;       // 필드명 = rawSymbol (getter와 이름 불일치)
@JsonProperty("symbol")
public String getSymbol() {...} // 별도 @JsonProperty → 독립적으로 직렬화됨
```
Jackson은 필드와 getter가 같은 이름이면 필드의 `access` 속성을 getter에도 적용한다. `rawXxx` 패턴으로 이 연결을 끊는 것이 핵심.

### 3-5. Kafka Producer 설정
```java
ACKS_CONFIG = "1"            // 리더 확인만 (모든 복제본 확인 불필요 — 단일 브로커 환경)
LINGER_MS_CONFIG = "10"      // 10ms 동안 메시지 모아서 배치 전송
BATCH_SIZE_CONFIG = "16384"  // 배치당 최대 16KB
COMPRESSION_TYPE_CONFIG = "lz4"  // LZ4: 빠른 압축/해제, 네트워크 대역폭 절감
ENABLE_IDEMPOTENCE_CONFIG = "false"  // 단일 브로커 개발환경, 멱등성 불필요
```
- `acks=all`은 복제본이 없는 개발환경에서 의미 없음
- lz4는 gzip 대비 압축률은 낮지만 CPU 오버헤드가 적어 고빈도 메시지에 적합

### 3-6. Kafka 파티션 키 설계
| 토픽 | 키 | 이유 |
|------|-----|------|
| crypto-ticker | `"BTCUSDT"` | 같은 심볼은 같은 파티션 → 순서 보장 |
| crypto-kline | `"BTCUSDT@1m"` | 심볼+인터벌 조합으로 세분화 |

kline 키에 인터벌을 포함한 이유: 1m, 5m, 15m, 1h를 같은 파티션에 넣으면 순서는 보장되지만 파티션 편중이 발생할 수 있음. 심볼+인터벌로 더 균등하게 분산.

### 3-7. KlineEventHandler의 is_closed 필터링
```java
if (!event.isClosed()) {
    skippedCount.incrementAndGet();
    return;  // 미확정 캔들은 Kafka에 보내지 않음
}
```
미확정 캔들 이벤트는 1분봉 기준 약 60배 더 많다. Producer 단에서 필터링하면 Kafka, Consumer, DB 전체 부하를 동시에 줄인다.

---

## 4. 변수명 규칙

| 패턴 | 예시 | 이유 |
|------|------|------|
| `raw` 접두사 | `rawSymbol`, `kline` | Jackson WRITE_ONLY 필드 — getter와 분리 명시 |
| camelCase 클래스명 | `BinanceWebSocketClient` | Java 표준 |
| UPPER_SNAKE_CASE 상수 | `RECONNECT_DELAY_SEC`, `MAX_RECONNECT_ATTEMPTS` | Java 상수 관례 |
| 동사+명사 메서드 | `buildStreamUrl()`, `scheduleReconnect()` | 행위 명시 |
| 단순 명사 필드 | `running`, `scheduler`, `reconnectAttempts` | 상태/자원 명시 |

---

## 5. 성능 분석

| 항목 | 현재 | 개선 여지 |
|------|------|----------|
| WS 연결 수 | 2개 (ticker, kline 각 1개) | Combined Stream으로 1개로 통합 가능 (현재 50 스트림, 상한 1024) |
| 역직렬화 | ObjectMapper 매 메시지마다 호출 | ObjectMapper는 스레드 안전하므로 싱글턴 재사용 중 (이미 최적) |
| Kafka 전송 | 비동기 콜백 | 배치 전송으로 네트워크 왕복 최소화 |
| is_closed 필터 | Handler 레이어 | Consumer까지 내려보내지 않아 최적 위치 |

---

## 6. 개선 여지

1. **단일 WS 연결로 통합**: ticker와 kline 클라이언트를 하나의 Combined Stream으로 합치면 연결 관리 포인트가 줄어듦
2. **설정 외부화**: `KAFKA_BOOTSTRAP_SERVERS`가 코드에 하드코딩 → `config.properties` 또는 환경변수로
3. **Graceful Shutdown 완성도**: `producer.flush()` 이후 `close()`는 구현되어 있으나, WS 메시지 수신 중 종료 시 마지막 배치 유실 가능성 존재 (현재 데이터 손실 허용 가능 수준)
