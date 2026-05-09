export interface Ticker {
  symbol: string;
  lastPrice: string;
  priceChange: string;
  priceChangePct: string;
  baseVolume: string;
  createdAt?: string;
  eventTime?: string;
}

export interface ProducerStatus {
  instanceId: string;
  weight: number;
  state: 'ACTIVE' | 'STANDBY' | 'PREPARING' | 'OFFLINE';
  receivedAt: string;
  updatedAt: string;
}

export interface Kline {
  id: number;
  symbol: string;
  intervalType: string;
  openTime: string;
  closeTime: string;
  openPrice: number;
  highPrice: number;
  lowPrice: number;
  closePrice: number;
  volume: number;
  quoteVolume: number;
  tradeCount: number;
}
