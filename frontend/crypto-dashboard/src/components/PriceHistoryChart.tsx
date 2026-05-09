import { useEffect, useRef } from 'react';
import { createChart, ColorType, CrosshairMode, LineStyle } from 'lightweight-charts';
import { Ticker } from '../types';

interface Props { data: Ticker[]; }

function toUnix(createdAt: string): number {
  return Math.floor(new Date(createdAt.replace(' ', 'T')).getTime() / 1000);
}

export default function PriceHistoryChart({ data }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef     = useRef<ReturnType<typeof createChart> | null>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const seriesRef    = useRef<any>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#13131a' },
        textColor: '#9090a8',
      },
      grid: {
        vertLines: { color: '#1e1e2e', style: LineStyle.Dotted },
        horzLines: { color: '#1e1e2e', style: LineStyle.Dotted },
      },
      crosshair: { mode: CrosshairMode.Magnet },
      rightPriceScale: { borderColor: '#2a2a3a' },
      timeScale: {
        borderColor: '#2a2a3a',
        timeVisible: true,
        secondsVisible: true,
      },
      width:  containerRef.current.clientWidth,
      height: containerRef.current.clientHeight,
    });

    const series = chart.addAreaSeries({
      lineColor:       '#6366f1',
      topColor:        '#6366f140',
      bottomColor:     '#6366f100',
      lineWidth:       2,
      priceLineVisible: false,
    });

    chartRef.current = chart;
    seriesRef.current = series;

    const onResize = () => {
      if (containerRef.current)
        chart.applyOptions({ width: containerRef.current.clientWidth });
    };
    window.addEventListener('resize', onResize);

    return () => {
      window.removeEventListener('resize', onResize);
      chart.remove();
    };
  }, []);

  useEffect(() => {
    if (!seriesRef.current || !data.length) return;

    const points = data
      .filter(t => !!t.createdAt)
      .map(t => ({ time: toUnix(t.createdAt!), value: parseFloat(t.lastPrice) }))
      .sort((a, b) => a.time - b.time)
      .filter((p, i, arr) => i === 0 || p.time !== arr[i - 1].time);

    seriesRef.current.setData(points);
    chartRef.current?.timeScale().fitContent();
  }, [data]);

  return <div ref={containerRef} className="w-full h-full" />;
}
