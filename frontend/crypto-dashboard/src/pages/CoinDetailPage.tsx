import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchKlines, fetchLatestTickers } from '../api';
import { Kline, Ticker } from '../types';
import { COINS, INTERVALS } from '../constants';
import CandlestickChart from '../components/CandlestickChart';
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
  const [klines, setKlines] = useState<Kline[]>([]);
  const [ticker, setTicker] = useState<Ticker | null>(null);
  const [loading, setLoading] = useState(true);

  const coin = COINS.find(c => c.symbol === symbol);

  useEffect(() => {
    if (!symbol) return;
    fetchLatestTickers()
      .then(data => setTicker(data.find(t => t.symbol === symbol) ?? null))
      .catch(() => {});
  }, [symbol]);

  useEffect(() => {
    if (!symbol) return;
    setLoading(true);
    fetchKlines(symbol, interval, 200)
      .then(setKlines)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [symbol, interval]);

  if (!coin) return (
    <div className="flex items-center justify-center h-screen text-text-secondary">
      코인을 찾을 수 없습니다.
    </div>
  );

  const lastKline = klines[klines.length - 1];

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
                  <span className="text-3xl font-bold text-text-primary">{fmtPrice(ticker.price)}</span>
                  <PriceBadge percent={ticker.priceChangePercent} />
                </>
              ) : (
                <div className="h-9 w-40 bg-surface-elevated rounded animate-pulse" />
              )}
            </div>
          </div>
        </div>
        <IntervalTabs value={interval} options={INTERVALS} onChange={setInterval} />
      </section>

      {/* Chart */}
      <section className="h-[500px] md:h-[580px] w-full relative bg-bg-base border-y border-border-subtle">
        {lastKline && (
          <div className="absolute top-3 left-3 z-10 flex flex-wrap gap-x-5 gap-y-1 bg-surface-default/80 backdrop-blur-sm px-3 py-2 rounded-lg border border-border-subtle text-[11px]">
            <span className="text-text-secondary">시가 <span className="text-text-primary">{parseFloat(lastKline.open).toLocaleString()}</span></span>
            <span className="text-text-secondary">고가 <span className="text-status-up font-bold">{parseFloat(lastKline.high).toLocaleString()}</span></span>
            <span className="text-text-secondary">저가 <span className="text-status-down font-bold">{parseFloat(lastKline.low).toLocaleString()}</span></span>
            <span className="text-text-secondary">종가 <span className="text-text-primary">{parseFloat(lastKline.close).toLocaleString()}</span></span>
            <span className="text-text-secondary border-l border-border-subtle pl-4">거래량 <span className="text-text-primary">{parseFloat(lastKline.volume).toFixed(2)}</span></span>
          </div>
        )}
        {loading
          ? <div className="flex items-center justify-center h-full text-text-secondary">차트 로딩 중...</div>
          : <CandlestickChart data={klines} />
        }
      </section>

      {/* Bottom */}
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
                  {parseFloat(ticker.volume).toLocaleString('en-US', { maximumFractionDigits: 0 })}
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
    </main>
  );
}
