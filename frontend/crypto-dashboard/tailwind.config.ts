import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        'status-up': '#e53e3e',
        'status-down': '#3b82f6',
        'status-neutral': '#8b8fa8',
        'accent-indigo': '#5c7cfa',
        'text-primary': '#f0f0f5',
        'text-secondary': '#9090a8',
        'surface-default': '#1a1a24',
        'surface-container': '#1e1f27',
        'surface-elevated': '#22222f',
        'surface-container-high': '#282931',
        'surface-container-low': '#1a1b23',
        'surface-variant': '#33343d',
        'border-subtle': '#2a2a3a',
        'outline-variant': '#444654',
        'on-surface': '#e2e1ec',
        'bg-base': '#0f0f14',
      },
      spacing: {
        xl: '32px',
        lg: '24px',
        md: '16px',
        sm: '8px',
        xs: '4px',
        gutter: '12px',
      },
      fontFamily: {
        sans: ['Work Sans', 'system-ui', 'sans-serif'],
        manrope: ['Manrope', 'sans-serif'],
      },
    },
  },
  plugins: [],
} satisfies Config;
