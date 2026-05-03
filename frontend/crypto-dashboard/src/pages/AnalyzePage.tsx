import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { COINS } from '../constants';

const MOCK_NEWS = [
  { title: '비트코인 현물 ETF 유입세 가속화... 사상 최고가 경신 눈앞', source: '코인데스크 코리아', time: '2시간 전', sentiment: '긍정' as const },
  { title: 'SEC 위원장 "암호화폐 시장 규제 강화 필요성" 재차 강조', source: '연합인포맥스', time: '4시간 전', sentiment: '부정' as const },
  { title: '블랙록 "비트코인은 더 이상 투기 자산 아닌 디지털 골드"', source: '블록미디어', time: '6시간 전', sentiment: '긍정' as const },
  { title: '미 연준 금리 동결 가능성 확대에 자산 시장 관망세 확산', source: '한국경제', time: '9시간 전', sentiment: '중립' as const },
  { title: '고래 주소 대규모 이체 포착... 시장 변동성 주의보', source: '디지털타임스', time: '12시간 전', sentiment: '중립' as const },
];

const MOCK_INSIGHTS = [
  '도미넌스 지표가 시장 주도권 강화 신호를 보이고 있습니다.',
  '주요 기술적 저항선 돌파 시 추가 상승 전망이 유력합니다.',
  'RSI 지표가 강세 모멘텀을 유지하고 있어 단기 상승 가능성 높음.',
  '장기 보유자들의 거래소 유출량이 증가 — 매집 신호.',
  '온체인 데이터 기준 강세 지속 예상, 단 단기 변동성 주의.',
];

const SENTIMENT = {
  긍정: { bg: 'bg-status-up/10',      text: 'text-status-up' },
  부정: { bg: 'bg-status-down/10',    text: 'text-status-down' },
  중립: { bg: 'bg-status-neutral/10', text: 'text-status-neutral' },
};

export default function AnalyzePage() {
  const { symbol } = useParams<{ symbol: string }>();
  const coin = COINS.find(c => c.symbol === symbol) ?? COINS[0];
  const [expanded, setExpanded] = useState(false);

  return (
    <main className="max-w-7xl mx-auto px-4 md:px-6 pt-6 pb-24">
      {/* Page Title */}
      <div className="mb-6 flex items-center gap-3">
        <div className="w-9 h-9 rounded-full flex items-center justify-center" style={{ backgroundColor: `${coin.color}20` }}>
          <span className="material-symbols-outlined text-lg" style={{ color: coin.color }}>{coin.icon}</span>
        </div>
        <h1 className="text-2xl font-extrabold text-text-primary font-manrope">{coin.korName} 분석</h1>
      </div>

      {/* ── 상단: 상세 분석 (클릭 시 토글) ── */}
      {expanded && (
        <div className="mb-4 bg-surface-default border border-accent-indigo/40 rounded-xl p-5 shadow-lg">
          <div className="flex justify-between items-center mb-3">
            <h2 className="font-semibold text-lg flex items-center gap-2">
              <span className="material-symbols-outlined text-accent-indigo">auto_awesome</span>
              상세 분석 리포트
            </h2>
            <button onClick={() => setExpanded(false)} className="text-text-secondary hover:text-white transition-colors">
              <span className="material-symbols-outlined">close</span>
            </button>
          </div>
          <p className="text-[14px] text-on-surface leading-relaxed mb-4">
            현재 {coin.korName}({coin.symbol.replace('USDT','')})의 시장 지표는 강력한 온체인 매집 신호를 나타내고 있습니다.
            장기 보유자들의 거래소 유출량이 증가하고 있으며, 이는 향후 가격 상승의 주요 동력으로 작용할 가능성이 큽니다.
            현재 기술적 분석 기준으로 주요 지지선은 유지되고 있으며, 단기적으로는 저항선 돌파 여부가 핵심 변수입니다.
          </p>
          <div className="grid grid-cols-3 gap-3 pt-3 border-t border-border-subtle">
            <div className="text-center">
              <div className="text-[11px] text-text-secondary mb-1">변동성</div>
              <div className="text-status-neutral font-bold">보통</div>
            </div>
            <div className="text-center">
              <div className="text-[11px] text-text-secondary mb-1">청산 위험</div>
              <div className="text-status-down font-bold">낮음</div>
            </div>
            <div className="text-center">
              <div className="text-[11px] text-text-secondary mb-1">투심 지수</div>
              <div className="text-status-up font-bold">탐욕</div>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-3">
        {/* ── 왼쪽: AI 투자 조언 요약 (5줄 + 클릭 → 상세) ── */}
        <div className="lg:col-span-7 flex flex-col gap-3">
          <div className="bg-surface-default border border-border-subtle rounded-xl overflow-hidden" style={{ boxShadow: '0 4px 24px rgba(229,62,62,0.12)' }}>
            <div className="px-5 py-4 border-b border-border-subtle flex justify-between items-center bg-surface-container-high/40">
              <div className="flex items-center gap-2">
                <span className="material-symbols-outlined text-accent-indigo">psychology</span>
                <h2 className="font-semibold text-lg">AI 투자 조언</h2>
              </div>
              <div className="flex items-center gap-2 bg-status-up/15 border border-status-up/30 px-3 py-1 rounded-full">
                <span className="w-2 h-2 rounded-full bg-status-up animate-pulse" />
                <span className="text-[12px] font-semibold text-status-up">강세</span>
              </div>
            </div>
            <div className="p-5">
              <ul className="space-y-3">
                {MOCK_INSIGHTS.map((insight, i) => (
                  <li key={i} className="flex items-start gap-3">
                    <span className="mt-0.5 w-5 h-5 rounded-full bg-accent-indigo/20 text-accent-indigo text-[11px] font-bold flex items-center justify-center shrink-0">{i + 1}</span>
                    <span className="text-[14px] text-on-surface leading-relaxed">{insight}</span>
                  </li>
                ))}
              </ul>
              <button
                onClick={() => setExpanded(true)}
                className="mt-5 w-full py-2.5 border border-accent-indigo/50 text-accent-indigo text-[13px] font-semibold rounded-lg hover:bg-accent-indigo/10 transition-colors flex items-center justify-center gap-2"
              >
                <span className="material-symbols-outlined text-[18px]">expand_more</span>
                상세 분석 보기
              </button>
              <p className="mt-3 text-[11px] text-text-secondary text-center flex items-center justify-center gap-1">
                <span className="material-symbols-outlined text-xs">info</span>
                Claude API 연동 예정 · 현재 예시 데이터
              </p>
            </div>
          </div>

          {/* 시장 심리 분포 */}
          <div className="bg-surface-default border border-border-subtle rounded-xl p-5">
            <h3 className="font-semibold text-base mb-4">시장 심리 분포</h3>
            <div className="flex h-2.5 w-full rounded-full overflow-hidden mb-3">
              <div className="bg-status-up h-full" style={{ width: '65%' }} />
              <div className="bg-status-neutral h-full" style={{ width: '20%' }} />
              <div className="bg-status-down h-full" style={{ width: '15%' }} />
            </div>
            <div className="flex justify-between text-[11px]">
              <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-status-up" />긍정 65%</span>
              <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-status-neutral" />중립 20%</span>
              <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-status-down" />부정 15%</span>
            </div>
          </div>
        </div>

        {/* ── 오른쪽: 관련 뉴스 ── */}
        <div className="lg:col-span-5">
          <div className="bg-surface-default border border-border-subtle rounded-xl overflow-hidden">
            <div className="px-5 py-4 border-b border-border-subtle flex items-center gap-2 bg-surface-container-high/40">
              <span className="material-symbols-outlined text-accent-indigo">newspaper</span>
              <h2 className="font-semibold text-lg">관련 뉴스</h2>
            </div>
            <div className="divide-y divide-border-subtle">
              {MOCK_NEWS.map((news, i) => {
                const sc = SENTIMENT[news.sentiment];
                return (
                  <div key={i} className="p-5 hover:bg-surface-container transition-colors cursor-pointer group">
                    <div className="flex justify-between items-start gap-4 mb-2">
                      <h3 className="font-semibold text-[14px] text-text-primary line-clamp-2 leading-snug group-hover:text-accent-indigo transition-colors">
                        {news.title}
                      </h3>
                      <span className={`${sc.bg} ${sc.text} text-[11px] font-semibold px-2 py-0.5 rounded shrink-0`}>
                        {news.sentiment}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 text-[11px] text-text-secondary">
                      <span>{news.source}</span>
                      <span className="w-1 h-1 rounded-full bg-outline-variant" />
                      <span>{news.time}</span>
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="p-4 text-center bg-surface-container-high/20">
              <button className="text-accent-indigo text-[12px] font-semibold hover:underline">뉴스 더보기</button>
            </div>
          </div>
        </div>
      </div>
    </main>
  );
}
