import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Ticker } from '../types';
import { fetchTickerHistory } from '../api';
import { COINS } from '../constants';
import PriceBadge from './PriceBadge';

function Sparkline({ data, isUp }: { data: number[]; isUp: boolean }) {
  if (data.length < 2) return <div className="h-12 w-full" />;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const W = 100, H = 40;
  const points = data
    .map((v, i) => `${(i / (data.length - 1)) * W},${H - ((v - min) / range) * H}`)
    .join(' ');
  const color = isUp ? '#e53e3e' : '#3b82f6';
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-12" preserveAspectRatio="none">
      <polyline points={points} fill="none" stroke={color} strokeWidth="1.5" opacity="0.85" />
    </svg>
  );
}

function fmtPrice(p: string) {
  const n = parseFloat(p);
  if (n >= 10000) return `$${Math.round(n).toLocaleString()}`;
  if (n >= 1)     return `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  return `$${n.toFixed(4)}`;
}

interface Props { ticker: Ticker; flash?: boolean; }

export default function CoinCard({ ticker, flash }: Props) {
  const navigate = useNavigate();
  const [history, setHistory] = useState<number[]>([]);
  const coin = COINS.find(c => c.symbol === ticker.symbol);
  const isUp = parseFloat(ticker.priceChangePercent) >= 0;

  useEffect(() => {
    fetchTickerHistory(ticker.symbol, 30)
      .then(data => setHistory(data.map(t => parseFloat(t.price))))
      .catch(() => {});
  }, [ticker.symbol]);

  const borderColor = isUp ? '#e53e3e' : '#3b82f6';
  const glowColor   = isUp ? 'rgba(229,62,62,0.35)' : 'rgba(59,130,246,0.35)';

  return (
    <div
      onClick={() => navigate(`/coin/${ticker.symbol}`)}
      className={`bg-surface-default rounded-xl border border-border-subtle p-4 hover:bg-surface-elevated transition-all duration-200 cursor-pointer shadow-lg group relative overflow-hidden ${flash ? (isUp ? 'flash-up' : 'flash-down') : ''}`}
    >
      <div className="flex justify-between items-start mb-3">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full flex items-center justify-center" style={{ backgroundColor: `${coin?.color}20` }}>
            <span className="material-symbols-outlined" style={{ color: coin?.color }}>{coin?.icon}</span>
          </div>
          <div>
            <h3 className="font-semibold text-text-primary leading-tight">{ticker.symbol.replace('USDT', '')}</h3>
            <p className="text-[11px] text-text-secondary">{coin?.name}</p>
          </div>
        </div>
        <PriceBadge percent={ticker.priceChangePercent} />
      </div>

      <div className="mb-1">
        <span className="text-[22px] font-bold text-text-primary tracking-tight">{fmtPrice(ticker.price)}</span>
      </div>

      <Sparkline data={history} isUp={isUp} />

      <div className="flex justify-between mt-1 text-[11px] text-text-secondary">
        <span>거래량 24h</span>
        <span>{parseFloat(ticker.volume).toLocaleString('en-US', { maximumFractionDigits: 0 })}</span>
      </div>

      <div
        className="absolute -bottom-1 left-0 right-0 h-1 opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        style={{ backgroundColor: borderColor, boxShadow: `0 0 12px ${glowColor}` }}
      />
    </div>
  );
}
