export const API_BASE = 'http://localhost:8081';
export const WS_URL = 'ws://localhost:8081/ws';

export const COINS = [
  { symbol: 'BTCUSDT', name: 'Bitcoin',      korName: '비트코인',   color: '#f7931a', icon: 'currency_bitcoin' },
  { symbol: 'ETHUSDT', name: 'Ethereum',     korName: '이더리움',   color: '#627eea', icon: 'diamond' },
  { symbol: 'BNBUSDT', name: 'BNB',          korName: '바이낸스코인', color: '#f3ba2f', icon: 'grid_view' },
  { symbol: 'SOLUSDT', name: 'Solana',       korName: '솔라나',    color: '#14f195', icon: 'wb_sunny' },
  { symbol: 'ADAUSDT', name: 'Cardano',      korName: '카르다노',   color: '#0077ff', icon: 'blur_on' },
  { symbol: 'XRPUSDT', name: 'Ripple',       korName: '리플',     color: '#9090a8', icon: 'token' },
  { symbol: 'DOTUSDT', name: 'Polkadot',     korName: '폴카닷',    color: '#e6007a', icon: 'radio_button_checked' },
  { symbol: 'AVAXUSDT', name: 'Avalanche',   korName: '아발란체',   color: '#e84142', icon: 'change_history' },
  { symbol: 'ATOMUSDT', name: 'Cosmos',      korName: '코스모스',   color: '#7a81ad', icon: 'hub' },
  { symbol: 'NEARUSDT', name: 'NEAR Protocol', korName: '니어',   color: '#e8e8e8', icon: 'waves' },
] as const;

export const INTERVALS = [
  { label: '1분',  value: '1m' },
  { label: '5분',  value: '5m' },
  { label: '15분', value: '15m' },
  { label: '1시간', value: '1h' },
] as const;
