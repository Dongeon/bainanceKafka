import { BrowserRouter, Routes, Route } from 'react-router-dom';
import GNB from './components/GNB';
import BottomTabBar from './components/BottomTabBar';
import HomePage from './pages/HomePage';
import CoinDetailPage from './pages/CoinDetailPage';
import AnalyzePage from './pages/AnalyzePage';

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-bg-base text-text-primary">
        <GNB />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/coin/:symbol" element={<CoinDetailPage />} />
          <Route path="/analyze/:symbol" element={<AnalyzePage />} />
          <Route path="/analyze" element={<AnalyzePage />} />
        </Routes>
        <BottomTabBar />
      </div>
    </BrowserRouter>
  );
}
