import axios from 'axios';
import { Ticker, Kline, ProducerStatus } from './types';
import { API_BASE } from './constants';

const http = axios.create({ baseURL: API_BASE });

export const fetchLatestTickers = () =>
  http.get<Ticker[]>('/api/tickers/latest').then(r => r.data);

export const fetchTickerHistory = (symbol: string, limit = 30) =>
  http.get<Ticker[]>(`/api/tickers/${symbol}/history`, { params: { limit } }).then(r => r.data);

export const fetchKlines = (symbol: string, interval: string, limit = 200) =>
  http.get<Kline[]>(`/api/klines/${symbol}`, { params: { interval, limit } }).then(r => r.data);

export const fetchProducerStatus = () =>
  http.get<ProducerStatus[]>('/api/producers/status').then(r => r.data);

export const fetchBinanceKlines = (
  symbol: string,
  interval: string,
  limit = 500,
  endTime?: number,
): Promise<unknown[][]> => {
  const params = new URLSearchParams({ symbol: symbol.toUpperCase(), interval, limit: String(limit) });
  if (endTime != null) params.set('endTime', String(endTime));
  return fetch(`https://api.binance.com/api/v3/klines?${params.toString()}`).then(r => r.json());
};
