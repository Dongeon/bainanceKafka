import { useState, useEffect, useRef } from 'react';
import { createChart, ColorType, CrosshairMode, LineStyle, IChartApi, UTCTimestamp } from 'lightweight-charts';
import {
  SMA, EMA, RSI, MACD, BollingerBands, Stochastic,
  CCI, ATR, OBV, WilliamsR, MFI, VWAP, PSAR, KeltnerChannels,
} from 'technicalindicators';
import { fetchBinanceKlines } from '../api';

type ChartType = 'candlestick' | 'hollow' | 'heikinashi' | 'bar' | 'line' | 'area' | 'baseline';
type OverlayKey = 'sma5'|'sma10'|'sma20'|'sma60'|'ema9'|'ema21'|'ema55'|'bb'|'vwap'|'sar'|'keltner';
type SubKey = 'rsi'|'macd'|'stoch'|'cci'|'atr'|'obv'|'wr'|'mfi';

interface BarData {
  _t: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface Props { symbol: string; interval: string; }

function parseBinance(raw: unknown[][]): BarData[] {
  return (raw as [number, string, string, string, string, string][])
    .map(r => ({
      _t: Math.floor(r[0] / 1000) as UTCTimestamp,
      open:   parseFloat(r[1]),
      high:   parseFloat(r[2]),
      low:    parseFloat(r[3]),
      close:  parseFloat(r[4]),
      volume: parseFloat(r[5]),
    }))
    .sort((a, b) => a._t - b._t)
    .filter((b, i, arr) => i === 0 || b._t !== arr[i - 1]._t);
}

function calcHA(ks: BarData[]) {
  const res = [];
  let po = (ks[0].open + ks[0].close) / 2;
  let pc = (ks[0].open + ks[0].high + ks[0].low + ks[0].close) / 4;
  res.push({ open: po, high: ks[0].high, low: ks[0].low, close: pc });
  for (let i = 1; i < ks.length; i++) {
    const hc = (ks[i].open + ks[i].high + ks[i].low + ks[i].close) / 4;
    const ho = (po + pc) / 2;
    res.push({ open: ho, high: Math.max(ks[i].high, ho, hc), low: Math.min(ks[i].low, ho, hc), close: hc });
    po = ho; pc = hc;
  }
  return res;
}

function align<T>(times: UTCTimestamp[], vals: T[]): { time: UTCTimestamp; val: T }[] {
  const offset = times.length - vals.length;
  return vals.map((val, i) => ({ time: times[offset + i], val }));
}

const CHART_TYPES: { key: ChartType; label: string }[] = [
  { key: 'candlestick', label: '캔들' },
  { key: 'hollow',      label: '할로우' },
  { key: 'heikinashi',  label: '헤이킨아시' },
  { key: 'bar',         label: '바' },
  { key: 'line',        label: '라인' },
  { key: 'area',        label: '에리어' },
  { key: 'baseline',    label: '베이스라인' },
];
const OVERLAYS: { key: OverlayKey; label: string; color: string }[] = [
  { key: 'sma5',    label: 'SMA5',   color: '#f59e0b' },
  { key: 'sma10',   label: 'SMA10',  color: '#10b981' },
  { key: 'sma20',   label: 'SMA20',  color: '#6366f1' },
  { key: 'sma60',   label: 'SMA60',  color: '#ec4899' },
  { key: 'ema9',    label: 'EMA9',   color: '#f97316' },
  { key: 'ema21',   label: 'EMA21',  color: '#14b8a6' },
  { key: 'ema55',   label: 'EMA55',  color: '#8b5cf6' },
  { key: 'bb',      label: 'BB',     color: '#6366f1' },
  { key: 'vwap',    label: 'VWAP',   color: '#fbbf24' },
  { key: 'sar',     label: 'SAR',    color: '#f43f5e' },
  { key: 'keltner', label: '켈트너',  color: '#06b6d4' },
];
const SUBS: { key: SubKey; label: string }[] = [
  { key: 'rsi',   label: 'RSI' },
  { key: 'macd',  label: 'MACD' },
  { key: 'stoch', label: 'Stoch' },
  { key: 'cci',   label: 'CCI' },
  { key: 'atr',   label: 'ATR' },
  { key: 'obv',   label: 'OBV' },
  { key: 'wr',    label: 'WR' },
  { key: 'mfi',   label: 'MFI' },
];

const BG = '#0f0f14', GRID = '#1a1a2e', TXT = '#9090a8', BORD = '#2a2a3a';
const UP = '#26a69a', DN = '#ef5350';

function baseOpts(w: number, h: number) {
  return {
    layout: { background: { type: ColorType.Solid, color: BG }, textColor: TXT },
    grid: { vertLines: { color: GRID, style: LineStyle.Dotted }, horzLines: { color: GRID, style: LineStyle.Dotted } },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: { borderColor: BORD },
    timeScale: { borderColor: BORD, timeVisible: true, secondsVisible: false },
    width: w, height: h,
  };
}
function fmt(n: number) { return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }

export default function CandlestickChart({ symbol, interval }: Props) {
  const [chartType, setChartType] = useState<ChartType>('candlestick');
  const [activeOv,  setActiveOv]  = useState<Set<OverlayKey>>(new Set());
  const [activeSub, setActiveSub] = useState<Set<SubKey>>(new Set());
  const [bars,        setBars]        = useState<BarData[]>([]);
  const [loading,     setLoading]     = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);

  const mainRef  = useRef<HTMLDivElement>(null);
  const tipRef   = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mainSer   = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const volSer    = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ovSers    = useRef<Map<OverlayKey, any[]>>(new Map());
  const subCharts = useRef<Map<SubKey, IChartApi>>(new Map());
  const subConts  = useRef<Map<SubKey, HTMLDivElement | null>>(new Map());

  const symbolRef      = useRef(symbol);
  const intervalRef    = useRef(interval);
  const isFetchingRef  = useRef(false);
  const oldestTimeRef  = useRef<number | null>(null);
  const prependedRef   = useRef(0);

  useEffect(() => { symbolRef.current   = symbol;   }, [symbol]);
  useEffect(() => { intervalRef.current = interval; }, [interval]);

  // ── 초기 로드 (symbol / interval 변경 시) ─────────────────────────────────
  useEffect(() => {
    setLoading(true);
    isFetchingRef.current = false;
    oldestTimeRef.current = null;
    prependedRef.current  = 0;
    setBars([]);
    fetchBinanceKlines(symbol, interval, 500)
      .then(raw => {
        const parsed = parseBinance(raw);
        setBars(parsed);
        if (parsed.length) oldestTimeRef.current = parsed[0]._t * 1000;
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [symbol, interval]);

  // ── 메인 차트 생성 (마운트 1회) ───────────────────────────────────────────
  useEffect(() => {
    if (!mainRef.current) return;
    const c = createChart(mainRef.current, baseOpts(mainRef.current.clientWidth, mainRef.current.clientHeight));
    chartRef.current = c;
    const vol = c.addHistogramSeries({ priceScaleId: 'vol' });
    c.priceScale('vol').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
    volSer.current = vol;

    // 왼쪽 끝 도달 시 과거 데이터 추가 로드
    c.timeScale().subscribeVisibleLogicalRangeChange(range => {
      if (!range || isFetchingRef.current || !oldestTimeRef.current) return;
      if (range.from > 10) return;
      isFetchingRef.current = true;
      setLoadingMore(true);
      fetchBinanceKlines(symbolRef.current, intervalRef.current, 500, oldestTimeRef.current - 1)
        .then(raw => {
          const newBars = parseBinance(raw);
          if (!newBars.length) {
            isFetchingRef.current = false;
            setLoadingMore(false);
            return;
          }
          oldestTimeRef.current = newBars[0]._t * 1000;
          prependedRef.current  = newBars.length;
          setBars(prev => {
            const merged = [...newBars, ...prev];
            const seen   = new Set<number>();
            return merged.filter(b => {
              const t = b._t as number;
              if (seen.has(t)) return false;
              seen.add(t);
              return true;
            });
          });
          setLoadingMore(false);
          // isFetchingRef는 series useEffect에서 range 복원 후 해제
        })
        .catch(() => { isFetchingRef.current = false; setLoadingMore(false); });
    });

    // 크로스헤어 툴팁
    c.subscribeCrosshairMove(param => {
      const el = tipRef.current;
      if (!el) return;
      if (!param.point || !param.time || !param.seriesData) { el.style.display = 'none'; return; }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const cd = param.seriesData.get(mainSer.current) as any;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const vd = param.seriesData.get(volSer.current) as any;
      if (!cd) { el.style.display = 'none'; return; }
      const o = cd.open ?? cd.value, h = cd.high ?? cd.value, l = cd.low ?? cd.value, cl = cd.close ?? cd.value;
      const isUp = cl >= o;
      const unix = param.time as number, d = new Date(unix * 1000), pad = (n: number) => String(n).padStart(2, '0');
      const ts = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
      el.innerHTML = `
        <div style="color:${TXT};font-size:10px;margin-bottom:4px">${ts}</div>
        <div style="display:grid;grid-template-columns:auto auto;gap:2px 12px;font-size:11px">
          <span style="color:${TXT}">시가</span><span style="color:#e8e8e8">${fmt(o)}</span>
          <span style="color:${TXT}">고가</span><span style="color:${UP};font-weight:700">${fmt(h)}</span>
          <span style="color:${TXT}">저가</span><span style="color:${DN};font-weight:700">${fmt(l)}</span>
          <span style="color:${TXT}">종가</span><span style="color:${isUp ? UP : DN};font-weight:700">${fmt(cl)}</span>
          ${vd ? `<span style="color:${TXT}">거래량</span><span style="color:#e8e8e8">${(vd.value as number).toFixed(4)}</span>` : ''}
        </div>`;
      el.style.display = 'block';
      const r  = mainRef.current!.getBoundingClientRect();
      const tw = el.offsetWidth || 165, th = el.offsetHeight || 100;
      let lx = param.point.x + 16, ly = param.point.y - th / 2;
      if (lx + tw > r.width) lx = param.point.x - tw - 16;
      if (ly < 0) ly = 0; if (ly + th > r.height) ly = r.height - th;
      el.style.left = `${lx}px`; el.style.top = `${ly}px`;
    });

    const onResize = () => { if (mainRef.current) c.applyOptions({ width: mainRef.current.clientWidth }); };
    window.addEventListener('resize', onResize);
    return () => { window.removeEventListener('resize', onResize); c.remove(); chartRef.current = null; };
  }, []);

  // ── 메인 시리즈 (차트 타입 전환 + 데이터) ────────────────────────────────
  useEffect(() => {
    const c = chartRef.current;
    if (!c || !bars.length) return;

    const prevRange = c.timeScale().getVisibleLogicalRange();
    if (mainSer.current) { try { c.removeSeries(mainSer.current); } catch { /**/ } mainSer.current = null; }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let s: any;
    if (chartType === 'candlestick') {
      s = c.addCandlestickSeries({ upColor: UP, downColor: DN, borderUpColor: UP, borderDownColor: DN, wickUpColor: UP, wickDownColor: DN });
    } else if (chartType === 'hollow') {
      s = c.addCandlestickSeries({ upColor: 'transparent', downColor: DN, borderUpColor: UP, borderDownColor: DN, wickUpColor: UP, wickDownColor: DN });
    } else if (chartType === 'heikinashi') {
      s = c.addCandlestickSeries({ upColor: UP, downColor: DN, borderUpColor: UP, borderDownColor: DN, wickUpColor: UP, wickDownColor: DN });
    } else if (chartType === 'bar') {
      s = c.addBarSeries({ upColor: UP, downColor: DN });
    } else if (chartType === 'line') {
      s = c.addLineSeries({ color: '#6366f1', lineWidth: 2, priceLineVisible: false });
    } else if (chartType === 'area') {
      s = c.addAreaSeries({ lineColor: '#6366f1', topColor: '#6366f140', bottomColor: '#6366f100', lineWidth: 2, priceLineVisible: false });
    } else {
      s = c.addBaselineSeries({ topLineColor: UP, topFillColor1: `${UP}30`, topFillColor2: `${UP}05`, bottomLineColor: DN, bottomFillColor1: `${DN}05`, bottomFillColor2: `${DN}30`, priceLineVisible: false });
    }
    mainSer.current = s;

    if (chartType === 'heikinashi') {
      const ha = calcHA(bars);
      s.setData(bars.map((k, i) => ({ time: k._t, ...ha[i] })));
    } else if (['line', 'area', 'baseline'].includes(chartType)) {
      s.setData(bars.map(k => ({ time: k._t, value: k.close })));
    } else {
      s.setData(bars.map(k => ({ time: k._t, open: k.open, high: k.high, low: k.low, close: k.close })));
    }

    volSer.current?.setData(bars.map(k => ({
      time: k._t, value: k.volume, color: k.close >= k.open ? `${UP}55` : `${DN}55`,
    })));

    const prepended = prependedRef.current;
    if (prepended > 0) {
      prependedRef.current   = 0;
      isFetchingRef.current  = false;
      if (prevRange) {
        c.timeScale().setVisibleLogicalRange({ from: prevRange.from + prepended, to: prevRange.to + prepended });
      }
    } else if (prevRange) {
      c.timeScale().setVisibleLogicalRange(prevRange);
    } else {
      c.timeScale().fitContent();
    }
  }, [chartType, bars]);

  // ── 오버레이 지표 ─────────────────────────────────────────────────────────
  useEffect(() => {
    const c = chartRef.current;
    if (!c || !bars.length) return;
    ovSers.current.forEach(arr => arr.forEach(s => { try { c.removeSeries(s); } catch { /**/ } }));
    ovSers.current.clear();

    const ts = bars.map(k => k._t);
    const cl = bars.map(k => k.close);
    const hi = bars.map(k => k.high);
    const lo = bars.map(k => k.low);
    const vo = bars.map(k => k.volume);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mkLine = (vals: number[], color: string, dashed = false): any => {
      const s = c.addLineSeries({ color, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, lineStyle: dashed ? LineStyle.Dashed : LineStyle.Solid });
      s.setData(align(ts, vals).map(p => ({ time: p.time, value: p.val })));
      return s;
    };

    activeOv.forEach(key => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const arr: any[] = [];
      if (key === 'sma5')    arr.push(mkLine(SMA.calculate({ period: 5,  values: cl }), '#f59e0b'));
      if (key === 'sma10')   arr.push(mkLine(SMA.calculate({ period: 10, values: cl }), '#10b981'));
      if (key === 'sma20')   arr.push(mkLine(SMA.calculate({ period: 20, values: cl }), '#6366f1'));
      if (key === 'sma60')   arr.push(mkLine(SMA.calculate({ period: 60, values: cl }), '#ec4899'));
      if (key === 'ema9')    arr.push(mkLine(EMA.calculate({ period: 9,  values: cl }), '#f97316'));
      if (key === 'ema21')   arr.push(mkLine(EMA.calculate({ period: 21, values: cl }), '#14b8a6'));
      if (key === 'ema55')   arr.push(mkLine(EMA.calculate({ period: 55, values: cl }), '#8b5cf6'));
      if (key === 'vwap')    arr.push(mkLine(VWAP.calculate({ high: hi, low: lo, close: cl, volume: vo }), '#fbbf24', true));
      if (key === 'sar')     arr.push(mkLine(PSAR.calculate({ step: 0.02, max: 0.2, high: hi, low: lo }), '#f43f5e'));
      if (key === 'bb') {
        const bb = BollingerBands.calculate({ period: 20, values: cl, stdDev: 2 });
        arr.push(mkLine(bb.map(v => v.upper),  '#6366f180', true));
        arr.push(mkLine(bb.map(v => v.middle), '#6366f1',   true));
        arr.push(mkLine(bb.map(v => v.lower),  '#6366f180', true));
      }
      if (key === 'keltner') {
        const kc = KeltnerChannels.calculate({ maPeriod: 20, atrPeriod: 14, multiplier: 1.5, high: hi, low: lo, close: cl, useSMA: false });
        arr.push(mkLine(kc.map(v => v.upper),  '#06b6d480', true));
        arr.push(mkLine(kc.map(v => v.middle), '#06b6d4'));
        arr.push(mkLine(kc.map(v => v.lower),  '#06b6d480', true));
      }
      ovSers.current.set(key, arr);
    });
  }, [activeOv, bars]);

  // ── 서브 차트 ─────────────────────────────────────────────────────────────
  useEffect(() => {
    const main = chartRef.current;
    if (!main) return;
    subCharts.current.forEach(ch => ch.remove());
    subCharts.current.clear();
    if (!bars.length) return;

    const ts = bars.map(k => k._t);
    const cl = bars.map(k => k.close);
    const hi = bars.map(k => k.high);
    const lo = bars.map(k => k.low);
    const vo = bars.map(k => k.volume);

    const mkSubLine = (ch: IChartApi, vals: number[], color: string, width: 1 | 2 = 2, dashed = false) => {
      const s = ch.addLineSeries({ color, lineWidth: width, priceLineVisible: false, lastValueVisible: false, lineStyle: dashed ? LineStyle.Dashed : LineStyle.Solid });
      s.setData(align(ts, vals).map(p => ({ time: p.time, value: p.val })));
      return s;
    };

    activeSub.forEach(key => {
      const cont = subConts.current.get(key);
      if (!cont) return;
      const ch = createChart(cont, baseOpts(cont.clientWidth, cont.clientHeight));
      subCharts.current.set(key, ch);

      if (key === 'rsi') {
        mkSubLine(ch, RSI.calculate({ period: 14, values: cl }), '#6366f1');
        [70, 30].forEach((v, i) => {
          const ref = ch.addLineSeries({ color: i === 0 ? `${DN}60` : `${UP}60`, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, lineStyle: LineStyle.Dashed });
          ref.setData(ts.map(t => ({ time: t, value: v })));
        });
      } else if (key === 'macd') {
        const m = MACD.calculate({ fastPeriod: 12, slowPeriod: 26, signalPeriod: 9, values: cl, SimpleMAOscillator: false, SimpleMASignal: false });
        mkSubLine(ch, m.map(v => v.MACD ?? 0), '#6366f1');
        mkSubLine(ch, m.map(v => v.signal ?? 0), '#f97316', 1);
        const hs = ch.addHistogramSeries({ priceLineVisible: false });
        hs.setData(align(ts, m).map(p => ({ time: p.time, value: p.val.histogram ?? 0, color: (p.val.histogram ?? 0) >= 0 ? `${UP}80` : `${DN}80` })));
      } else if (key === 'stoch') {
        const st = Stochastic.calculate({ high: hi, low: lo, close: cl, period: 14, signalPeriod: 3 });
        mkSubLine(ch, st.map(v => v.k), '#6366f1');
        mkSubLine(ch, st.map(v => v.d), '#f97316', 1);
      } else if (key === 'cci') {
        mkSubLine(ch, CCI.calculate({ high: hi, low: lo, close: cl, period: 20 }), '#14b8a6');
      } else if (key === 'atr') {
        mkSubLine(ch, ATR.calculate({ high: hi, low: lo, close: cl, period: 14 }), '#8b5cf6');
      } else if (key === 'obv') {
        const obvVals = OBV.calculate({ close: cl, volume: vo });
        const s = ch.addLineSeries({ color: '#fbbf24', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
        s.setData(obvVals.map((v, i) => ({ time: ts[i], value: v })));
      } else if (key === 'wr') {
        mkSubLine(ch, WilliamsR.calculate({ high: hi, low: lo, close: cl, period: 14 }), '#ec4899');
      } else if (key === 'mfi') {
        mkSubLine(ch, MFI.calculate({ high: hi, low: lo, close: cl, volume: vo, period: 14 }), '#f97316');
      }

      ch.timeScale().fitContent();
      const range = main.timeScale().getVisibleLogicalRange();
      if (range) ch.timeScale().setVisibleLogicalRange(range);
      main.timeScale().subscribeVisibleLogicalRangeChange(r => { if (r) ch.timeScale().setVisibleLogicalRange(r); });

      const onResize = () => { if (cont) ch.applyOptions({ width: cont.clientWidth }); };
      window.addEventListener('resize', onResize);
    });
  }, [activeSub, bars]);

  const toggleOv  = (k: OverlayKey) => setActiveOv(p  => { const n = new Set(p); n.has(k) ? n.delete(k) : n.add(k); return n; });
  const toggleSub = (k: SubKey)     => setActiveSub(p => { const n = new Set(p); n.has(k) ? n.delete(k) : n.add(k); return n; });

  return (
    <div className="flex flex-col w-full" style={{ background: BG }}>
      {/* 컨트롤 바 */}
      <div className="px-3 py-2 flex flex-wrap items-center gap-x-3 gap-y-1.5 border-b" style={{ borderColor: BORD }}>
        <div className="flex items-center gap-0.5">
          {CHART_TYPES.map(ct => (
            <button key={ct.key} onClick={() => setChartType(ct.key)}
              className={`px-2.5 py-1 text-[11px] rounded font-medium transition-colors ${chartType === ct.key ? 'bg-[#6366f1] text-white' : 'text-[#9090a8] hover:text-white hover:bg-white/5'}`}>
              {ct.label}
            </button>
          ))}
        </div>
        <div className="w-px h-4 shrink-0" style={{ background: BORD }} />
        <div className="flex items-center gap-0.5 flex-wrap">
          {OVERLAYS.map(ov => (
            <button key={ov.key} onClick={() => toggleOv(ov.key)}
              className="px-2.5 py-1 text-[11px] rounded font-medium transition-all"
              style={activeOv.has(ov.key) ? { background: `${ov.color}25`, color: ov.color, border: `1px solid ${ov.color}50` } : { color: TXT }}>
              {ov.label}
            </button>
          ))}
        </div>
        <div className="w-px h-4 shrink-0" style={{ background: BORD }} />
        <div className="flex items-center gap-0.5 flex-wrap">
          {SUBS.map(si => (
            <button key={si.key} onClick={() => toggleSub(si.key)}
              className={`px-2.5 py-1 text-[11px] rounded font-medium transition-colors ${activeSub.has(si.key) ? 'text-white border border-[#3a3a5a]' : 'text-[#9090a8] hover:text-white hover:bg-white/5'}`}
              style={activeSub.has(si.key) ? { background: '#2a2a3a' } : {}}>
              {si.label}
            </button>
          ))}
        </div>
      </div>

      {/* 메인 차트 */}
      <div ref={mainRef} className="w-full h-[380px] relative">
        {loading && !bars.length && (
          <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 10, color: TXT, fontSize: 13 }}>
            차트 로딩 중...
          </div>
        )}
        {loadingMore && (
          <div style={{ position: 'absolute', top: 8, left: 8, zIndex: 10, color: TXT, fontSize: 11, background: 'rgba(15,15,20,0.85)', padding: '2px 8px', borderRadius: 4 }}>
            과거 데이터 로딩 중...
          </div>
        )}
        <div ref={tipRef} style={{
          display: 'none', position: 'absolute', pointerEvents: 'none', zIndex: 10,
          background: 'rgba(15,15,20,0.92)', border: `1px solid ${BORD}`,
          borderRadius: 8, padding: '8px 12px', minWidth: 160,
        }} />
      </div>

      {/* 서브 차트 패널 */}
      {SUBS.filter(si => activeSub.has(si.key)).map(si => (
        <div key={si.key} className="w-full border-t" style={{ borderColor: BORD }}>
          <div className="px-3 py-0.5 text-[10px] font-semibold" style={{ color: TXT }}>{si.label}</div>
          <div ref={el => { subConts.current.set(si.key, el); }} className="w-full h-[100px]" />
        </div>
      ))}
    </div>
  );
}
