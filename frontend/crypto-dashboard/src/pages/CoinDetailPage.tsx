import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchLatestTickers, fetchTickerHistory } from '../api';
import { Ticker } from '../types';
import { COINS, INTERVALS } from '../constants';
import CandlestickChart from '../components/CandlestickChart';
import PriceHistoryChart from '../components/PriceHistoryChart';
import IntervalTabs from '../components/IntervalTabs';
import PriceBadge from '../components/PriceBadge';

function fmtPrice(p: string) {
  const n = parseFloat(p);
  if (n >= 10000) return `$${Math.round(n).toLocaleString()}`;
  if (n >= 1) return `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  return `$${n.toFixed(4)}`;
}

export default function CoinDetailPage() {
  const { symbol } = useParams<{ symbol: string }>();
  const [interval, setInterval] = useState('1m');
  const [ticker, setTicker] = useState<Ticker | null>(null);
  const [history, setHistory] = useState<Ticker[]>([]);

  const coin = COINS.find(c => c.symbol === symbol);

  useEffect(() => {
    if (!symbol) return;
    fetchLatestTickers()
      .then(data => setTicker(data.find(t => t.symbol === symbol) ?? null))
      .catch(() => {});
  }, [symbol]);

  useEffect(() => {
    if (!symbol) return;
    fetchTickerHistory(symbol, 30)
      .then(setHistory)
      .catch(() => {});
  }, [symbol]);


  if (!coin) return (
    <div className="flex items-center justify-center h-screen text-text-secondary">
      코인을 찾을 수 없습니다.
    </div>
  );

  return (
    <main className="max-w-7xl mx-auto pb-20">
      {/* Header */}
      <section className="px-4 md:px-6 py-6 flex flex-col md:flex-row md:items-end justify-between gap-4">
        <div className="flex items-center gap-4">
          <div className="w-12 h-12 rounded-full flex items-center justify-center" style={{ backgroundColor: `${coin.color}20` }}>
            <span className="material-symbols-outlined text-2xl" style={{ color: coin.color }}>{coin.icon}</span>
          </div>
          <div>
            <h1 className="text-xl font-bold text-text-primary font-manrope">{coin.korName}</h1>
            <div className="flex items-center gap-3 mt-1">
              {ticker ? (
                <>
                  <span className="text-3xl font-bold text-text-primary">{fmtPrice(ticker.lastPrice)}</span>
                  <PriceBadge percent={ticker.priceChangePct} />
                </>
              ) : (
                <div className="h-9 w-40 bg-surface-elevated rounded animate-pulse" />
              )}
            </div>
          </div>
        </div>
        <IntervalTabs value={interval} options={INTERVALS} onChange={setInterval} />
      </section>

      {/* Candlestick Chart */}
      <section className="w-full bg-bg-base border-y border-border-subtle">
        <CandlestickChart symbol={symbol!} interval={interval} />
      </section>

      {/* Stats */}
      <section className="px-4 md:px-6 py-4 grid grid-cols-1 md:grid-cols-4 gap-3">
        <Link
          to={`/analyze/${symbol}`}
          className="py-4 bg-accent-indigo hover:bg-accent-indigo/90 text-white font-semibold rounded-xl text-center flex items-center justify-center gap-2 transition-all active:scale-[0.98] shadow-lg"
        >
          <span className="material-symbols-outlined">psychology</span>
          AI 분석 보기
        </Link>
        {ticker && (
          <>
            <div className="bg-surface-elevated p-4 rounded-xl border border-border-subtle">
              <span className="text-[11px] text-text-secondary block mb-1">24H 변동</span>
              <div className="flex items-center gap-2">
                <span className={`text-lg font-bold ${parseFloat(ticker.priceChange) >= 0 ? 'text-status-up' : 'text-status-down'}`}>
                  {parseFloat(ticker.priceChange) >= 0 ? '+' : ''}{parseFloat(ticker.priceChange).toFixed(2)}
                </span>
                <span className={`material-symbols-outlined ${parseFloat(ticker.priceChange) >= 0 ? 'text-status-up' : 'text-status-down'}`}>
                  {parseFloat(ticker.priceChange) >= 0 ? 'trending_up' : 'trending_down'}
                </span>
              </div>
            </div>
            <div className="bg-surface-elevated p-4 rounded-xl border border-border-subtle">
              <span className="text-[11px] text-text-secondary block mb-1">24H 거래량</span>
              <div className="flex items-center gap-2">
                <span className="text-lg font-bold text-text-primary">
                  {parseFloat(ticker.baseVolume).toLocaleString('en-US', { maximumFractionDigits: 0 })}
                </span>
                <span className="material-symbols-outlined text-status-neutral">bar_chart</span>
              </div>
            </div>
            <div className="bg-surface-elevated p-4 rounded-xl border border-border-subtle">
              <span className="text-[11px] text-text-secondary block mb-1">최근 업데이트</span>
              <div className="flex items-center gap-2">
                <span className="text-base font-bold text-text-primary">{ticker.createdAt?.substring(11, 19)}</span>
                <span className="material-symbols-outlined text-status-neutral">schedule</span>
              </div>
            </div>
          </>
        )}
      </section>

      {/* Price History Chart */}
      {history.length > 0 && (
        <section className="px-4 md:px-6 py-4">
          <h2 className="text-sm font-semibold text-text-secondary mb-3 flex items-center gap-2">
            <span className="material-symbols-outlined text-base">show_chart</span>
            시세 추이
          </h2>
          <div className="h-48 rounded-xl overflow-hidden border border-border-subtle">
            <PriceHistoryChart data={history} />
          </div>
        </section>
      )}

      {/* Price History List */}
      <section className="px-4 md:px-6 py-4">
        <h2 className="text-sm font-semibold text-text-secondary mb-3 flex items-center gap-2">
          <span className="material-symbols-outlined text-base">history</span>
          최근 시세 기록
        </h2>
        <div className="bg-surface-elevated rounded-xl border border-border-subtle overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border-subtle text-text-secondary text-xs">
                <th className="text-left px-4 py-3 font-medium">시간</th>
                <th className="text-right px-4 py-3 font-medium">가격</th>
                <th className="text-right px-4 py-3 font-medium">변동</th>
                <th className="text-right px-4 py-3 font-medium hidden md:table-cell">거래량</th>
              </tr>
            </thead>
            <tbody>
              {history.length === 0 ? (
                <tr>
                  <td colSpan={4} className="text-center py-8 text-text-secondary text-xs">데이터 없음</td>
                </tr>
              ) : (
                [...history].reverse().map((h, i) => {
                  const change = parseFloat(h.priceChange);
                  const pct = parseFloat(h.priceChangePct);
                  const isUp = change >= 0;
                  return (
                    <tr key={i} className="border-b border-border-subtle/40 last:border-0 hover:bg-surface-container/50 transition-colors">
                      <td className="px-4 py-3 text-text-secondary font-mono text-xs">
                        {h.createdAt?.substring(11, 19) ?? '-'}
                      </td>
                      <td className="px-4 py-3 text-right font-bold text-text-primary font-mono">
                        {fmtPrice(h.lastPrice)}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span className={`font-mono text-xs font-semibold ${isUp ? 'text-status-up' : 'text-status-down'}`}>
                          {isUp ? '+' : ''}{pct.toFixed(2)}%
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right text-text-secondary font-mono text-xs hidden md:table-cell">
                        {parseFloat(h.baseVolume).toLocaleString('en-US', { maximumFractionDigits: 0 })}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
