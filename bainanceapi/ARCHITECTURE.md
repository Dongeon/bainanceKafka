# bainanceapi 아키텍처

## 1. 모듈 책임

MariaDB에서 데이터를 읽어 REST API와 WebSocket을 통해 프론트엔드에 제공한다. **읽기 전용**이며 DB 쓰기는 하지 않는다.

---

## 2. 컴포넌트 구성

```
BainanceApiApplication (@EnableScheduling)
│
├── config/
│   ├── WebSocketConfig    ← STOMP 엔드포인트(/ws), 브로커(/topic), CORS 설정
│   └── WebConfig          ← REST CORS 설정
│
├── controller/
│   ├── TickerController   ← GET /api/tickers/latest, /api/tickers/{symbol}/history
│   └── KlineController    ← GET /api/klines/{symbol}
│
├── service/
│   ├── TickerService      ← 비즈니스 로직 (Pageable 변환)
│   ├── KlineService       ← 비즈니스 로직 (interval 파라미터 처리)
│   └── TickerBroadcastService ← @Scheduled(fixedDelay=1000) WS 브로드캐스트
│
├── repository/
│   ├── TickerRepository   ← JPA, Native SQL (findLatestPerSymbol)
│   └── KlineRepository    ← JPA, JPQL (findBySymbol...)
│
└── model/
    ├── Ticker             ← @Entity, ticker 테이블 매핑
    └── Kline              ← @Entity, kline 테이블 매핑
```

Controller → Service → Repository → DB의 전형적인 레이어드 아키텍처.

---

## 3. 핵심 설계 결정

### 3-1. Spring Data JPA 선택
Consumer 모듈은 JDBC 직접 사용(쓰기 성능 중시), API 모듈은 JPA 사용(읽기 편의성 중시). 역할이 달라 기술 선택도 다름.

JPA 장점:
- `findBySymbolOrderByEventTimeDesc(String, Pageable)` — 메서드명으로 쿼리 자동 생성
- 엔티티 → JSON 직렬화 자동 처리
- 페이징 처리가 `Pageable` 객체 하나로 단순화

### 3-2. findLatestPerSymbol의 Native SQL 선택
```sql
SELECT t.* FROM ticker t
INNER JOIN (
    SELECT symbol, MAX(event_time) AS max_time
    FROM ticker
    GROUP BY symbol
) latest ON t.symbol = latest.symbol AND t.event_time = latest.max_time
```
JPQL 방식(서브쿼리):
```sql
-- 이렇게 하면 N+1 발생
SELECT t FROM Ticker t WHERE t.eventTime = (
    SELECT MAX(t2.eventTime) FROM Ticker t2 WHERE t2.symbol = t.symbol
)
```
JPQL 방식은 심볼마다 MAX 스캔을 반복해 10번의 서브쿼리가 실행된다. Native SQL의 GROUP BY → JOIN은 한 번의 인덱스 스캔으로 처리. `idx_symbol_event_time` 인덱스가 있으면 극적 성능 차이.

1초마다 브로드캐스트 서비스에서 이 쿼리를 호출하므로 성능이 중요.

### 3-3. @Scheduled(fixedDelay = 1000) vs fixedRate
```java
@Scheduled(fixedDelay = 1000)   // 이전 실행 완료 후 1초 뒤 시작
// vs
@Scheduled(fixedRate = 1000)    // 1초마다 무조건 시작 (이전 실행 중에도)
```
DB 조회 + WS 전송이 1초를 넘으면 `fixedRate`는 중복 실행된다. `fixedDelay`는 항상 1초 간격을 유지. 실시간성보다 안정성 우선.

### 3-4. STOMP over WebSocket (SockJS 없음)
```java
registry.addEndpoint("/ws")
        .setAllowedOriginPatterns("*");
        // .withSockJS() ← 없음
```
SockJS는 WebSocket 미지원 브라우저를 위한 폴백(Long Polling, etc.)이다. 현대 브라우저는 모두 WebSocket을 지원하므로 불필요. React 클라이언트는 `brokerURL: 'ws://localhost:8081/ws'`로 직접 연결.

SockJS를 쓰면 클라이언트도 `webSocketURL` 대신 `sockJSFactoryURL`을 써야 하고 라이브러리 의존성이 늘어난다.

### 3-5. REST API 설계
| 엔드포인트 | 설계 근거 |
|-----------|----------|
| `GET /api/tickers/latest` | 10개 심볼의 현재가 한 번에 조회 (화면 초기 로드용) |
| `GET /api/tickers/{symbol}/history?limit=100` | 스파크라인 히스토리 조회, limit으로 유연성 |
| `GET /api/klines/{symbol}?interval=1m&limit=200` | 캔들차트 데이터, 인터벌과 개수 모두 파라미터화 |

WS는 push(서버→클라이언트), REST는 pull(클라이언트→서버) 명확히 분리.

---

## 4. 변수명 규칙

| 패턴 | 예시 | 이유 |
|------|------|------|
| `@Query` Native SQL | `findLatestPerSymbol` | 메서드명이 동작을 설명 |
| DTO 없이 엔티티 직접 반환 | `List<Ticker>` | 현재 API 응답과 엔티티 구조 일치, 별도 DTO 불필요 |
| `intervalType` 필드명 | `Kline.intervalType` | DB 컬럼 `interval_type`과 매핑, SQL 예약어 `interval` 회피 |

---

## 5. 성능 분석

| 엔드포인트 | 쿼리 횟수 | 인덱스 활용 |
|-----------|-----------|------------|
| `/api/tickers/latest` | 1회 GROUP BY JOIN | `idx_symbol_event_time` |
| `/api/tickers/{symbol}/history` | 1회 WHERE + ORDER + LIMIT | `idx_symbol_event_time` |
| `/api/klines/{symbol}` | 1회 WHERE + ORDER + LIMIT | `idx_symbol_interval` |
| WS 브로드캐스트 | 1회/초 (위와 동일) | 동일 |

브로드캐스트가 1초마다 DB를 찌르는 구조다. 연결된 클라이언트가 1명이든 1000명이든 DB 쿼리는 1초에 1번. 클라이언트 수 증가에 따른 DB 부하가 없다.

---

## 6. 개선 여지

1. **캐싱 추가**: `findLatestPerSymbol` 결과를 1초 TTL 캐시에 저장하면 REST 직접 호출 시 DB 쿼리 생략 가능 (`@Cacheable` + Caffeine)
2. **DTO 분리**: 현재 엔티티를 직접 JSON으로 반환 중. 응답 스펙 변경 시 엔티티도 변경해야 함 → DTO 분리가 안전하지만 현재 규모에서는 오버엔지니어링
3. **CORS 운영 설정**: `setAllowedOriginPatterns("*")`는 개발용. 운영 배포 시 프론트 도메인 명시 필요
4. **Kline WebSocket 브로드캐스트**: 현재 kline은 REST 폴링만 지원. 실시간 캔들 업데이트는 `/topic/klines/{symbol}` 토픽 추가 필요
5. **인터벌 파라미터 검증**: `GET /api/klines/{symbol}?interval=abc`와 같은 잘못된 인터벌이 들어오면 DB에서 빈 결과만 반환. 입력 검증 및 400 응답 처리 권장
