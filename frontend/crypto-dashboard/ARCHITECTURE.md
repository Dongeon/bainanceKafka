# frontend/crypto-dashboard 아키텍처

## 1. 모듈 책임

Spring Boot API의 REST와 WebSocket을 소비하여 실시간 암호화폐 대시보드 UI를 렌더링한다. 거래 기능 없는 **순수 데이터 시각화 앱**이다.

---

## 2. 기술 스택 선택

| 기술 | 선택 이유 |
|------|----------|
| React 18 | 상태 기반 UI, 실시간 데이터 업데이트에 자연스러운 패턴 |
| TypeScript | API 응답 타입 안전성, IDE 자동완성 (특히 `Ticker`, `Kline` 인터페이스) |
| Vite | CRA 대비 10배 이상 빠른 HMR, ES Module 기반 빌드 |
| Tailwind CSS | 커스텀 컬러 토큰(status-up, status-down)으로 한국 핀테크 색상 체계 적용 |
| @stomp/stompjs | SockJS 의존 없이 순수 WebSocket 위 STOMP 구현, 재연결 자동 처리 |
| TradingView Lightweight Charts v4 | 오픈소스 중 성능 최상위, Canvas 기반 60fps 렌더링 |
| axios | Fetch API 대비 인터셉터, 타입 추론, 자동 JSON 파싱 편의성 |
| React Router v6 | SPA 라우팅, nested routes, useParams |

---

## 3. 디렉토리 구조

```
src/
├── types.ts          ← 전역 타입 정의 (Ticker, Kline)
├── constants.ts      ← COINS 배열, INTERVALS, API_BASE, WS_URL
├── api.ts            ← axios 인스턴스 + REST 호출 함수
├── App.tsx           ← Router 설정, 페이지 라우팅
├── main.tsx          ← React 앱 진입점
│
├── hooks/
│   └── useWebSocket.ts ← STOMP 연결 + 상태 관리 커스텀 훅
│
├── components/
│   ├── GNB.tsx           ← 데스크탑 상단 네비게이션
│   ├── BottomTabBar.tsx  ← 모바일 하단 탭 (3개 탭)
│   ├── CoinCard.tsx      ← 코인 카드 (스파크라인 포함)
│   ├── CandlestickChart.tsx ← TradingView 캔들차트 래퍼
│   ├── IntervalTabs.tsx  ← 1분/5분/15분/1시간 탭
│   └── PriceBadge.tsx    ← 상승/하락 퍼센트 배지
│
└── pages/
    ├── HomePage.tsx      ← / (코인 카드 그리드)
    ├── CoinDetailPage.tsx ← /coin/:symbol (캔들차트)
    └── AnalyzePage.tsx   ← /analyze/:symbol (AI 분석 + 뉴스)
```

---

## 4. 핵심 설계 결정

### 4-1. useTickerStream 커스텀 훅
```typescript
export function useTickerStream() {
  const [tickers, setTickers] = useState<Record<string, Ticker>>({});
  const clientRef = useRef<Client | null>(null);
  // ...
  return { tickers, connected };
}
```
STOMP 연결 로직을 훅으로 분리한 이유:
- 컴포넌트가 "연결을 어떻게 하는지" 알 필요 없음 → 관심사 분리
- `useEffect` 클린업(`client.deactivate()`)으로 컴포넌트 언마운트 시 자동 연결 해제
- `Record<string, Ticker>` 구조 → 심볼 키로 O(1) 조회 (`tickers["BTCUSDT"]`)

### 4-2. CandlestickChart의 useEffect 분리
```typescript
// useEffect 1: 차트 인스턴스 초기화 (최초 1회)
useEffect(() => {
  const chart = createChart(containerRef.current, {...});
  chartRef.current = chart;
  return () => chart.remove();  // 클린업
}, []);  // 의존성 없음

// useEffect 2: 데이터 업데이트 (data 변경 시)
useEffect(() => {
  seriesRef.current?.setData(data.map(...));
}, [data]);
```
차트 인스턴스 생성과 데이터 주입을 분리한 이유:
- 차트 생성은 DOM이 준비된 후 1회만 필요
- 데이터만 바뀔 때 차트를 재생성하면 애니메이션과 줌 상태가 초기화됨
- `chartRef`, `seriesRef`로 인스턴스를 React state 밖에 보관 (state에 넣으면 불필요한 리렌더)

### 4-3. 한국 캔들 색상 (빨강=상승, 파랑=하락)
```typescript
chart.addCandlestickSeries({
  upColor: '#e53e3e',    // 빨강 = 상승
  downColor: '#3b82f6',  // 파랑 = 하락
  borderUpColor: '#e53e3e',
  borderDownColor: '#3b82f6',
  wickUpColor: '#e53e3e',
  wickDownColor: '#3b82f6',
});
```
한국(업비트, 빗썸) 표준. 미국(바이낸스, Coinbase)은 반대(초록=상승, 빨강=하락). Tailwind 토큰 `status-up: '#e53e3e'`, `status-down: '#3b82f6'`으로 컴포넌트 전반에 일관 적용.

### 4-4. openTime 단위 변환
```typescript
time: Math.floor(k.openTime / 1000)  // ms → 초
```
TradingView Lightweight Charts는 Unix 시간을 **초(seconds)** 단위로 받는다. Binance와 DB는 **밀리초(ms)** 단위로 저장. 변환 누락 시 차트 시간축이 2,554년대로 표시되는 버그 발생.

### 4-5. Tailwind 커스텀 컬러 토큰
```typescript
// tailwind.config.ts
colors: {
  'status-up': '#e53e3e',
  'status-down': '#3b82f6',
  'status-neutral': '#a0aec0',
  'accent-indigo': '#5c7cfa',
  'surface-default': '#161622',
  'text-primary': '#e2e8f0',
  // ...
}
```
컬러를 토큰화한 이유:
- 상승/하락 색을 바꾸려면 `tailwind.config.ts` 1곳만 수정
- 컴포넌트에서 `text-red-500` 같은 하드코딩 없이 의미 기반 클래스 사용

### 4-6. 모바일/데스크탑 반응형 레이아웃
```
모바일 (< md):  하단 탭바 (BottomTabBar) + 1열 카드 그리드
데스크탑 (≥ md): 상단 GNB + 다열 카드 그리드
```
```tsx
<div className="hidden md:block"><GNB /></div>
<div className="md:hidden fixed bottom-0 w-full"><BottomTabBar /></div>
```
한국 모바일 앱(업비트, 카카오페이) 패턴을 따름. 데스크탑에서 하단 탭바는 어색함.

---

## 5. 변수명 규칙

| 패턴 | 예시 | 이유 |
|------|------|------|
| PascalCase 컴포넌트 | `CoinCard`, `GNB`, `PriceBadge` | React 컴포넌트 관례 |
| camelCase 훅 | `useTickerStream`, `useWebSocket` | `use` 접두사로 훅 식별 |
| `Ref` 접미사 | `containerRef`, `chartRef`, `seriesRef` | `useRef` 반환값 명시 |
| 대문자 상수 | `COINS`, `INTERVALS`, `API_BASE`, `WS_URL` | 모듈 레벨 불변 설정값 |
| camelCase TS 인터페이스 필드 | `priceChangePercent`, `intervalType` | TS 관례, Java camelCase와 일치 |

---

## 6. 데이터 흐름

### 실시간 시세 (WebSocket)
```
Spring Boot → STOMP /topic/tickers → useTickerStream → tickers 상태
→ HomePage (CoinCard 그리드) + CoinDetailPage (헤더 현재가)
```

### 캔들차트 (REST 폴링)
```
사용자 인터벌 탭 클릭 → IntervalTabs → CoinDetailPage
→ fetchKlines(symbol, interval) → api.ts → axios → Spring Boot
→ CandlestickChart data prop → useEffect [data] → series.setData()
```

### AI 분석 (현재 MOCK)
```
AnalyzePage → MOCK_INSIGHTS, MOCK_NEWS (하드코딩)
→ 추후: fetchAnalysis(symbol) → /api/analyze/{symbol} → Claude API
```

---

## 7. 개선 여지

1. **스파크라인 데이터**: CoinCard의 스파크라인이 현재 MOCK 데이터. `fetchTickerHistory(symbol, 30)`으로 실제 히스토리 데이터 연결 필요
2. **에러 바운더리**: 차트 렌더링 실패 시 전체 페이지가 크래시할 수 있음. `<ErrorBoundary>` 컴포넌트 추가 권장
3. **로딩 상태**: `fetchKlines` 호출 중 로딩 스피너가 없어 UX 공백 발생
4. **타입 안전성**: `seriesRef.current`가 `any` 타입. `ISeriesApi<"Candlestick">` 로 구체화 가능
5. **환경변수**: `API_BASE`, `WS_URL`이 `constants.ts`에 하드코딩 → `.env`, `.env.production`으로 분리 권장
6. **Claude API 연동**: `AnalyzePage`의 `MOCK_INSIGHTS` → 백엔드 `/api/analyze/{symbol}` 연결 (최근 kline 데이터 기반 AI 분석)
