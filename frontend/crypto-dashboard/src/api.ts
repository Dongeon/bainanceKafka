import axios from 'axios';
import { Ticker, Kline } from './types';
import { API_BASE } from './constants';

const http = axios.create({ baseURL: API_BASE });

export const fetchLatestTickers = () =>
  http.get<Ticker[]>('/api/tickers/latest').then(r => r.data);

export const fetchTickerHistory = (symbol: string, limit = 30) =>
  http.get<Ticker[]>(`/api/tickers/${symbol}/history`, { params: { limit } }).then(r => r.data);

export const fetchKlines = (symbol: string, interval: string, limit = 200) =>
  http.get<Kline[]>(`/api/klines/${symbol}`, { params: { interval, limit } }).then(r => r.data);
