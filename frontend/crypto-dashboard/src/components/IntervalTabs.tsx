interface Option { label: string; value: string; }
interface Props { value: string; options: readonly Option[]; onChange: (v: string) => void; }

export default function IntervalTabs({ value, options, onChange }: Props) {
  return (
    <div className="flex items-center bg-surface-container-low p-1 rounded-lg border border-border-subtle">
      {options.map(opt => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`px-4 py-1.5 rounded-md text-[12px] font-semibold transition-colors ${
            value === opt.value
              ? 'bg-accent-indigo text-white shadow-lg'
              : 'text-text-secondary hover:text-white'
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
