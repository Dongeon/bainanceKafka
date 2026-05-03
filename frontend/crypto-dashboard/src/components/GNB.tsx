import { Link, useLocation } from 'react-router-dom';

export default function GNB() {
  const { pathname } = useLocation();
  const active = (p: string) =>
    p === '/' ? pathname === '/' : pathname.startsWith(p);

  return (
    <header className="hidden md:flex bg-surface-default border-b border-border-subtle justify-between items-center w-full px-6 h-14 sticky top-0 z-50 shadow-2xl">
      <div className="flex items-center gap-8">
        <Link to="/" className="text-xl font-black text-white tracking-tighter font-manrope">K-CRYPTO</Link>
        <nav className="flex h-full items-center gap-1">
          {[
            { to: '/', label: '홈' },
            { to: '/analyze', label: '분석' },
          ].map(({ to, label }) => (
            <Link
              key={to}
              to={to}
              className={`px-3 h-14 flex items-center text-sm font-medium transition-colors border-b-2 ${
                active(to)
                  ? 'text-white border-accent-indigo'
                  : 'text-slate-400 hover:text-white border-transparent'
              }`}
            >
              {label}
            </Link>
          ))}
        </nav>
      </div>
      <div className="flex items-center gap-1">
        <button className="p-2 text-slate-400 hover:text-white transition-colors rounded-lg hover:bg-surface-elevated">
          <span className="material-symbols-outlined">notifications</span>
        </button>
        <button className="p-2 text-slate-400 hover:text-white transition-colors rounded-lg hover:bg-surface-elevated">
          <span className="material-symbols-outlined">settings</span>
        </button>
      </div>
    </header>
  );
}
