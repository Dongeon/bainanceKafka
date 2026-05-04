export interface Ticker {
  symbol: string;
  lastPrice: string;
  priceChange: string;
  priceChangePct: string;
  baseVolume: string;
  createdAt?: string;
  eventTime?: string;
}

export interface Kline {
  symbol: string;
  intervalType: string;
  openTime: number;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
}
