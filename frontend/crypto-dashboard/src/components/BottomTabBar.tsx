import { NavLink } from 'react-router-dom';

const tabs = [
  { to: '/', label: '홈', icon: 'home', end: true },
  { to: '/analyze', label: '분석', icon: 'insights', end: false },
];

export default function BottomTabBar() {
  return (
    <nav className="md:hidden fixed bottom-0 left-0 w-full h-16 flex justify-around items-center px-4 bg-surface-default/95 backdrop-blur-md border-t border-border-subtle shadow-[0_-4px_20px_rgba(0,0,0,0.5)] z-50">
      {tabs.map(({ to, label, icon, end }) => (
        <NavLink
          key={to}
          to={to}
          end={end}
          className={({ isActive }) =>
            `flex flex-col items-center gap-0.5 transition-colors ${isActive ? 'text-accent-indigo' : 'text-slate-500'}`
          }
        >
          <span className="material-symbols-outlined text-2xl">{icon}</span>
          <span className="text-[10px] font-bold uppercase tracking-widest">{label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
