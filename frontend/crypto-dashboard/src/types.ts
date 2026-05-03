export interface Ticker {
  symbol: string;
  price: string;
  priceChange: string;
  priceChangePercent: string;
  volume: string;
  createdAt: string;
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
