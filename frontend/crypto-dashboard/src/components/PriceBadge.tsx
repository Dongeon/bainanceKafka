interface Props { percent: string; }

export default function PriceBadge({ percent }: Props) {
  const num = parseFloat(percent);
  const isUp = num >= 0;
  return (
    <div className={`flex items-center gap-0.5 px-2 py-0.5 rounded-full ${isUp ? 'bg-status-up/15' : 'bg-status-down/15'}`}>
      <span
        className={`material-symbols-outlined text-[14px] ${isUp ? 'text-status-up' : 'text-status-down'}`}
        style={{ fontVariationSettings: "'FILL' 1" }}
      >
        {isUp ? 'arrow_drop_up' : 'arrow_drop_down'}
      </span>
      <span className={`text-[12px] font-semibold ${isUp ? 'text-status-up' : 'text-status-down'}`}>
        {isUp ? '+' : ''}{num.toFixed(2)}%
      </span>
    </div>
  );
}
