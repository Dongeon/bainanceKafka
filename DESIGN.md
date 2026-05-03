# Crypto Dashboard — Design Spec

## Design System

- **Theme**: Dark, Korean fintech style (inspired by Upbit / Toss — NOT Binance)
- **Background**: #0f0f14
- **Surface**: #1a1a24
- **Surface elevated**: #22222f
- **Border**: #2a2a3a
- **Accent**: #5c7cfa (indigo-blue, not cyan)
- **Up (상승)**: #e53e3e — **RED** (Korean convention: red = up)
- **Down (하락)**: #3b82f6 — **BLUE** (Korean convention: blue = down)
- **Neutral**: #8b8fa8
- **Text primary**: #f0f0f5
- **Text secondary**: #9090a8
- **Font**: Pretendard, -apple-system, sans-serif
- **Border radius**: 12px (cards), 20px (pills/tabs)
- **Shadow**: 0 4px 24px rgba(0,0,0,0.4)

---

## Layout Philosophy
- 정보 밀도 높게 — 숫자 크고 명확하게
- 카드에 그라데이언트 테두리 효과 (상승시 red glow, 하락시 blue glow)
- 여백보다 데이터 우선
- 모바일 친화적 (하단 탭바)

---

## Pages

### Page 1 — 홈 (`/`)

**상단 요약 바** (full-width strip):
- 전체 시장 요약: "상승 7 / 하락 3" 텍스트
- 마지막 업데이트 시간 표시 (예: "14:23:01 기준")

**코인 카드 그리드** (3열 데스크탑 / 2열 태블릿 / 1열 모바일):
- 각 카드 구성:
  - 왼쪽: 코인 아이콘(원형) + 심볼 + 풀네임 (예: BTC · Bitcoin)
  - 오른쪽 상단: 현재가 (크고 bold)
  - 오른쪽 하단: 등락률 뱃지 — 상승이면 빨간 pill `▲ 2.04%`, 하락이면 파란 pill `▼ 1.23%`
  - 하단: 미니 스파크라인 차트 (30틱, 컬러는 등락 방향 따라감)
  - 거래량 (작은 글씨, muted)
- 실시간 WebSocket 업데이트: 가격 바뀔 때 숫자 flash 효과 (0.3초)
- 클릭 → `/coin/:symbol`

**코인 목록**: BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, ADAUSDT, XRPUSDT, DOTUSDT, AVAXUSDT, ATOMUSDT, NEARUSDT

---

### Page 2 — 코인 상세 (`/coin/:symbol`)

**상단 헤더 바**:
- 코인 아이콘 + 이름 + 현재가 + 등락률 뱃지
- 인터벌 탭 (pill 형태): `1분` | `5분` | `15분` | `1시간`
- 선택된 탭: accent 색 배경, 나머지: 투명

**차트 영역 (페이지의 70%)**:
- TradingView Lightweight Charts v4 캔들스틱
- 배경: #0f0f14
- 상승 캔들: #e53e3e (빨강)
- 하락 캔들: #3b82f6 (파랑)
- 그리드: #2a2a3a (매우 연하게)
- 크로스헤어 tooltip: OHLCV 전체 표시 (한국어 레이블: 시가/고가/저가/종가/거래량)
- 최신 캔들로 자동 스크롤

**하단 영역 (30%)**:
- "AI 분석 보기" 버튼 → `/analyze/:symbol` 이동
- 최근 거래 요약 (고가/저가/거래량 3개 수치 카드)

---

### Page 3 — AI 분석 (`/analyze/:symbol`)

**상단**: 코인명 + 현재가 + 등락률 (상세 페이지와 동일 헤더)

**두 카드 병렬 배치** (데스크탑), 세로 스택 (모바일):

**왼쪽 카드 — AI 투자 분석**:
- 헤더: "AI 분석" + 업데이트 시간
- 감성 뱃지: `강세` (빨강) / `약세` (파랑) / `중립` (회색)
- 분석 텍스트 본문
- 로딩 중: 스켈레톤 애니메이션 (shimmer 효과)

**오른쪽 카드 — 관련 뉴스**:
- 헤더: "관련 뉴스"
- 뉴스 아이템 (최대 5개, 이후 스크롤):
  - 제목 (링크, 2줄 clamp)
  - 출처 이름 + 시간 ("2시간 전")
  - 감성 태그: 긍정 / 부정 / 중립
- 구분선 있음

---

## 내비게이션

**데스크탑 — 상단 GNB**:
- 왼쪽: 로고 (텍스트 + accent 컬러 dot)
- 오른쪽: 홈 | 분석
- 현재 페이지: accent 컬러 underline

**모바일 — 하단 탭바** (Korean app 표준):
- 홈 | 분석 두 탭
- 아이콘 + 텍스트 레이블
- 선택된 탭: accent 컬러

---

## API (Backend: http://localhost:8081)

### REST
| Method | Path | 설명 |
|--------|------|------|
| GET | `/api/tickers/latest` | 전체 10개 코인 현재가 |
| GET | `/api/tickers/{symbol}/history?limit=100` | 틱 히스토리 |
| GET | `/api/klines/{symbol}?interval=1m&limit=200` | 캔들 데이터 |

### WebSocket
- URL: `ws://localhost:8081/ws`
- 프로토콜: STOMP
- 구독: `/topic/tickers`
- 주기: 1초마다 전체 ticker 배열 브로드캐스트

### Ticker 응답 예시
```json
{
  "symbol": "BTCUSDT",
  "price": "62450.12",
  "priceChange": "1250.34",
  "priceChangePercent": "2.04",
  "volume": "28341.92",
  "createdAt": "2026-05-03T14:23:01"
}
```

### Kline 응답 예시
```json
{
  "symbol": "BTCUSDT",
  "intervalType": "1m",
  "openTime": 1714694580000,
  "open": "62200.00",
  "high": "62500.00",
  "low": "62150.00",
  "close": "62450.12",
  "volume": "12.345"
}
```

---

## 컴포넌트 목록
- `CoinCard` — 실시간 가격 카드 + 스파크라인 + flash 효과
- `CandlestickChart` — TradingView v4, 빨강/파랑 캔들
- `IntervalTabs` — pill 형태 1분/5분/15분/1시간
- `PriceBadge` — 등락률 뱃지 (상승 빨강 / 하락 파랑)
- `NewsCard` — 뉴스 아이템 + 감성 태그
- `AnalysisCard` — AI 분석 + 스켈레톤
- `GNB` — 상단 네비게이션
- `BottomTabBar` — 모바일 하단 탭
- `LoadingSkeleton` — shimmer 로딩

---

## Tech Stack
- React 18 + TypeScript + Vite
- TradingView Lightweight Charts v4
- @stomp/stompjs + sockjs-client
- React Router v6
- axios
