# isetdxKafkaProducer 셋업 및 실행 가이드

## 전제 조건

| 항목 | 버전 |
|------|------|
| Java | 17 이상 |
| Kafka | localhost:9092 실행 중 |

---

## 디렉토리 구조

```
java_bainance/
├── IsetDx_kafka-leader-lib/     ← 리더 선출 라이브러리 (빌드 필요)
├── IsetDx_kafka-producer-lib/   ← Kafka Producer 래퍼 라이브러리 (빌드 필요)
└── isetdxKafkaProducer/         ← 메인 애플리케이션
    ├── kafka.conf                ← 설정 파일
    ├── build.gradle
    └── src/
```

---

## 1단계: 라이브러리 빌드

isetdxKafkaProducer는 두 개의 로컬 라이브러리 JAR에 의존한다.  
**최초 실행 또는 라이브러리 코드 변경 시 반드시 먼저 빌드해야 한다.**

```bash
# 프로젝트 루트(java_bainance/)에서 실행

cd IsetDx_kafka-leader-lib
./gradlew shadowJar
cd ..

cd IsetDx_kafka-producer-lib
./gradlew shadowJar
cd ..
```

빌드 결과물 위치:
- `IsetDx_kafka-leader-lib/build/libs/IsetDx_kafka-leader-lib-1.0.0.jar`
- `IsetDx_kafka-producer-lib/build/libs/IsetDx_kafka-producer-lib-1.0.0.jar`

---

## 2단계: kafka.conf 설정

`isetdxKafkaProducer/kafka.conf` 파일에서 주요 항목을 확인한다.

```properties
# Kafka 브로커
kafka.bootstrap.servers=localhost:9092

# 리더 선출
leader.instance.id=producer-1          # 인스턴스 식별자 (각 프로세스마다 고유하게)
leader.instance.weight=100             # 높을수록 split-brain 시 ACTIVE 우선
leader.prep.base.ms=5000               # 준비 구간 기본 시간 (ms)
leader.prep.jitter.ms=2000             # 준비 구간 랜덤 지터 (ms)
leader.activation.timeout.ms=15000    # STANDBY → ACTIVE 전환 타임아웃 (ms)
leader.heartbeat.interval.sec=5       # heartbeat 발송 주기 (초)
leader.heartbeat.topic=producer-heartbeat
leader.status.topic=producer-status   # 상태 이벤트 토픽

# Binance 수집 설정
binance.symbol.count=10               # 수집 심볼 수
binance.kline.intervals=1m,5m,15m,1h  # Kline 인터벌
binance.ticker.topic=crypto-ticker
binance.kline.topic=crypto-kline
```

> `leader.instance.id`와 `leader.instance.weight`는 실행 인수로 오버라이드 가능하다.  
> kafka.conf를 직접 수정하지 않아도 된다.

---

## 3단계: 실행

`isetdxKafkaProducer/` 디렉토리에서 실행한다.

```bash
cd isetdxKafkaProducer
```

### 단일 인스턴스 실행

```bash
./gradlew run
```

kafka.conf의 `leader.instance.id`, `leader.instance.weight` 기본값을 사용한다.

### 인스턴스 ID / Weight 지정 실행

```bash
./gradlew run --args="kafka.conf <instanceId> <weight>"
```

### Active-Standby 다중 인스턴스 (권장)

각각 별도 터미널에서 실행한다. weight가 높은 인스턴스가 ACTIVE 우선권을 가진다.

```bash
# 터미널 1 — weight 100 (최우선)
./gradlew run --args="kafka.conf producer-1 100"

# 터미널 2 — weight 90
./gradlew run --args="kafka.conf producer-2 90"

# 터미널 3 — weight 80
./gradlew run --args="kafka.conf producer-3 80"

# 터미널 4 — weight 70 (최하위)
./gradlew run --args="kafka.conf producer-4 70"
```

---

## 상태 전이 흐름

```
PREPARING ──(prep 경과, 외부 HB 없음)──▶ ACTIVE
PREPARING ──(외부 HB 감지)─────────────▶ STANDBY
STANDBY   ──(OFFLINE 이벤트 수신)──────▶ ACTIVE  (즉시 페일오버)
STANDBY   ──(activationTimeout 침묵)───▶ ACTIVE  (타임아웃 페일오버)
ACTIVE    ──(split-brain, 우선순위 패배)▶ STANDBY
```

- 시작 후 `prepBaseMs + jitter(0~prepJitterMs)` ms 동안 외부 heartbeat를 감지한다.
- 외부 heartbeat가 없으면 → **ACTIVE**, 있으면 → **STANDBY**
- ACTIVE 인스턴스가 종료되면 OFFLINE 이벤트를 `producer-status` 토픽에 발행한다.
- STANDBY 인스턴스는 OFFLINE 이벤트 수신 즉시 ACTIVE로 전환된다 (3ms 이내).

### Weight 기반 split-brain 해소

| 조건 | 결과 |
|------|------|
| 외부 weight > 내 weight | 내가 STANDBY (외부 우선) |
| 외부 weight == 내 weight | instanceId 사전순 비교, 더 작은 쪽이 ACTIVE 유지 |
| 외부 weight < 내 weight | 내가 ACTIVE 유지 |

---

## Kafka 토픽

| 토픽 | 용도 |
|------|------|
| `producer-heartbeat` | ACTIVE 인스턴스가 5초마다 발행 |
| `producer-status` | 모든 상태 전이 이벤트 발행 (PREPARING / ACTIVE / STANDBY / OFFLINE) |
| `crypto-ticker` | Binance 실시간 가격 데이터 |
| `crypto-kline` | Binance Kline(캔들) 데이터 |

---

## 종료

터미널에서 `Ctrl+C` 를 누르면 shutdown hook이 실행된다.

1. `producer-status` 토픽에 `OFFLINE` 이벤트 발행
2. STANDBY 인스턴스가 즉시 ACTIVE 전환
3. WebSocket 연결 해제, Kafka Producer flush 후 종료
