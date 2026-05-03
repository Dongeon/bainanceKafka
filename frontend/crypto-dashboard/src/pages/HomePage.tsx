import { useEffect, useRef, useState } from 'react';
import { useTickerStream } from '../hooks/useWebSocket';
import { fetchLatestTickers } from '../api';
import { Ticker } from '../types';
import { COINS } from '../constants';
import CoinCard from '../components/CoinCard';

export default function HomePage() {
  const { tickers, connected } = useTickerStream();
  const [initial, setInitial] = useState<Record<string, Ticker>>({});
  const [flashMap, setFlashMap] = useState<Record<string, boolean>>({});
  const prevPrices = useRef<Record<string, string>>({});
  const [time, setTime] = useState('');

  useEffect(() => {
    fetchLatestTickers()
      .then(data => {
        const m: Record<string, Ticker> = {};
        data.forEach(t => { m[t.symbol] = t; });
        setInitial(m);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    const id = setInterval(() => {
      const now = new Date();
      setTime(`${now.getHours().toString().padStart(2,'0')}:${now.getMinutes().toString().padStart(2,'0')}:${now.getSeconds().toString().padStart(2,'0')}`);
    }, 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const newFlash: Record<string, boolean> = {};
    Object.entries(tickers).forEach(([sym, t]) => {
      if (prevPrices.current[sym] !== t.price) {
        newFlash[sym] = true;
        prevPrices.current[sym] = t.price;
      }
    });
    if (Object.keys(newFlash).length) {
      setFlashMap(newFlash);
      setTimeout(() => setFlashMap({}), 400);
    }
  }, [tickers]);

  const merged = { ...initial, ...tickers };
  const upCount   = Object.values(merged).filter(t => parseFloat(t.priceChangePercent) >= 0).length;
  const downCount = Object.values(merged).filter(t => parseFloat(t.priceChangePercent) < 0).length;

  return (
    <main className="max-w-7xl mx-auto px-4 md:px-6 pt-6 pb-24 md:pb-12">
      {/* Summary Bar */}
      <div className="flex justify-between items-center mb-5 bg-surface-container rounded-lg px-4 py-3 border border-border-subtle">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-status-up" />
            <span className="text-status-up text-sm font-semibold">상승 {upCount}</span>
          </div>
          <div className="flex items-center gap-2 border-l border-border-subtle pl-4">
            <span className="w-2 h-2 rounded-full bg-status-down" />
            <span className="text-status-down text-sm font-semibold">하락 {downCount}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[11px] text-text-secondary">{time} 기준</span>
          <span className={`w-2 h-2 rounded-full ${connected ? 'bg-green-400 animate-pulse' : 'bg-red-500'}`} />
        </div>
      </div>

      {/* Coin Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
        {COINS.map(coin => {
          const ticker = merged[coin.symbol];
          if (!ticker) return (
            <div key={coin.symbol} className="bg-surface-default rounded-xl border border-border-subtle p-4 h-44 animate-pulse" />
          );
          return (
            <CoinCard key={coin.symbol} ticker={ticker} flash={flashMap[coin.symbol]} />
          );
        })}
      </div>
    </main>
  );
}
