import { useEffect, useRef, useState } from 'react';
import { useTickerStream } from '../hooks/useWebSocket';
import { fetchLatestTickers, fetchProducerStatus } from '../api';
import { Ticker, ProducerStatus } from '../types';
import { COINS } from '../constants';
import CoinCard from '../components/CoinCard';

const STATE_STYLE: Record<string, { dot: string; text: string; label: string }> = {
  ACTIVE:    { dot: 'bg-status-up animate-pulse',      text: 'text-status-up',      label: 'ACTIVE'    },
  STANDBY:   { dot: 'bg-status-neutral',               text: 'text-status-neutral', label: 'STANDBY'   },
  PREPARING: { dot: 'bg-text-secondary animate-pulse', text: 'text-text-secondary', label: 'PREPARING' },
  OFFLINE:   { dot: 'bg-status-down',                  text: 'text-status-down',    label: 'OFFLINE'   },
};

export default function HomePage() {
  const { tickers, connected } = useTickerStream();
  const [initial, setInitial] = useState<Record<string, Ticker>>({});
  const [flashMap, setFlashMap] = useState<Record<string, boolean>>({});
  const prevPrices = useRef<Record<string, string>>({});
  const [time, setTime] = useState('');
  const [producers, setProducers] = useState<ProducerStatus[]>([]);

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
    const load = () => fetchProducerStatus().then(setProducers).catch(() => {});
    load();
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
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
      if (prevPrices.current[sym] !== t.lastPrice) {
        newFlash[sym] = true;
        prevPrices.current[sym] = t.lastPrice;
      }
    });
    if (Object.keys(newFlash).length) {
      setFlashMap(newFlash);
      setTimeout(() => setFlashMap({}), 400);
    }
  }, [tickers]);

  const merged    = { ...initial, ...tickers };
  const upCount   = Object.values(merged).filter(t => parseFloat(t.priceChangePct) >= 0).length;
  const downCount = Object.values(merged).filter(t => parseFloat(t.priceChangePct) < 0).length;

  return (
    <main className="max-w-7xl mx-auto px-4 md:px-6 pt-6 pb-24 md:pb-12">
      {/* Summary Bar */}
      <div className="flex flex-wrap justify-between items-center mb-5 bg-surface-container rounded-lg px-4 py-3 border border-border-subtle gap-3">
        {/* 상승/하락 */}
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

        {/* 시간 + WS 상태 */}
        <div className="flex items-center gap-1 border-l border-border-subtle pl-4">
          <span className="text-[11px] text-text-secondary">{time} 기준</span>
          <span className={`w-2 h-2 rounded-full ${connected ? 'bg-green-400 animate-pulse' : 'bg-red-500'}`} />
        </div>

        {/* Producer 상태 */}
        {producers.length > 0 && (
          <div className="flex items-center gap-2">
            {producers.map(p => {
              const s = STATE_STYLE[p.state] ?? STATE_STYLE['OFFLINE'];
              return (
                <div key={p.instanceId}
                  className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-surface-elevated border border-border-subtle"
                  title={`${p.instanceId} · weight: ${p.weight} · ${p.receivedAt?.substring(11, 19)}`}
                >
                  <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${s.dot}`} />
                  <span className="text-[11px] text-text-secondary font-mono">{p.instanceId}</span>
                  <span className={`text-[10px] font-bold ${s.text}`}>{s.label}</span>
                </div>
              );
            })}
          </div>
        )}

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
