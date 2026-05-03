# 시스템 전체 아키텍처

## 1. 데이터 흐름 개요

```
Binance WebSocket (wss://stream.binance.com:9443)
    │
    ▼
[bainanceproducer]  ← OkHttp WebSocket + Jackson 역직렬화
    │  Kafka Topic: crypto-ticker  (파티션 키: symbol)
    │  Kafka Topic: crypto-kline   (파티션 키: symbol@interval)
    ▼
[bainanceconsumer]  ← Kafka Consumer + HikariCP + JDBC
    │
    ▼
MariaDB (localhost:33061)
    │  ticker 테이블 (초당 ~10건)
    │  kline  테이블 (분당 ~13건)
    ▼
[bainanceapi]       ← Spring Boot JPA + STOMP WebSocket
    │  REST: /api/tickers/latest, /api/tickers/{symbol}/history, /api/klines/{symbol}
    │  WebSocket: ws://localhost:8081/ws → /topic/tickers (1초 브로드캐스트)
    ▼
[frontend]          ← React + @stomp/stompjs + TradingView Lightweight Charts
```

---

## 2. 모듈 역할 분리

| 모듈 | 책임 | 기술 |
|------|------|------|
| bainanceproducer | 바이낸스 WS 구독 → Kafka 전송 | Java 17, OkHttp, Jackson, Kafka Producer |
| bainanceconsumer | Kafka 소비 → DB 저장 | Java 17, Kafka Consumer, HikariCP, JDBC |
| bainanceapi | DB 읽기 → REST + WS 제공 | Spring Boot 3, JPA, STOMP |
| frontend | 실시간 UI 렌더링 | React 18, TypeScript, Vite, TradingView |

각 모듈은 독립적으로 실행되며, Kafka와 MariaDB가 유일한 결합 지점이다.

---

## 3. 아키텍처 선택 근거

### 왜 Kafka인가?
- Producer와 Consumer를 완전히 분리 (Producer 재시작 → Consumer 영향 없음)
- Kafka 오프셋 덕분에 Consumer 장애 복구 후 미처리 메시지 재처리 가능
- 토픽 파티션 키로 심볼별 시계열 순서 보장

### 왜 Producer에서 is_closed 필터링인가?
- 바이낸스는 1분봉 기준 초당 약 10개 심볼 × 4 인터벌 = 초당 ~40개의 미확정 이벤트를 전송
- 인터벌 완료 시 확정 캔들 1개만 저장하면 충분 → Kafka 트래픽 99% 절감

### 왜 별도 Consumer 모듈인가?
- 쓰기(Consumer)와 읽기(API)가 같은 DB 커넥션 풀을 공유하지 않음
- Consumer 과부하 → API 응답 지연 없음

---

## 4. 성능 프로파일

| 단계 | 처리량 | 비고 |
|------|--------|------|
| WS 수신 (ticker) | ~10건/초 | 10 심볼 |
| WS 수신 (kline 미확정) | ~40건/초 | Producer 내에서 버림 |
| Kafka 전송 (ticker) | ~10건/초 | lz4 압축, batch 16KB |
| Kafka 전송 (kline 확정) | ~13건/분 | 분당 10×4인터벌 = 40건, 실제 발생 빈도 기준 |
| DB 쓰기 | ~10건/초 | HikariCP pool=5 |
| WS 브로드캐스트 | 1회/초 | fixedDelay=1000ms |

---

## 5. 포트 구성

| 서비스 | 포트 | 비고 |
|--------|------|------|
| Kafka | 9092 | |
| MariaDB | 33061 | Docker 컨테이너 |
| Spring Boot API | 8081 | 8080은 기존 Jetty 앱 점유 — 건드리지 말 것 |
| Frontend Vite | 3000 | |

---

## 6. 개선 여지

### 현재 한계
- **단일 Consumer 스레드**: Ticker 1개, Kline 1개로 고정. 파티션 수 늘려도 병렬 처리 불가
- **인메모리 STOMP 브로커**: 서버 재시작 시 구독 정보 소멸. 트래픽 증가 시 Redis pub/sub으로 교체 필요
- **하드코딩된 설정**: KAFKA_BOOTSTRAP_SERVERS, DB_URL 등이 소스 코드에 박혀 있음 → 환경변수 또는 설정 파일로 외부화 권장

### 추가 가능한 기능
- `/topic/klines/{symbol}` 실시간 캔들 업데이트 WS 토픽
- `/api/analyze/{symbol}` Claude API 기반 AI 분석 엔드포인트
- CryptoPanic 뉴스 API 연동
