package org.example.controller;

import org.example.model.Ticker;
import org.example.service.TickerService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 시세 데이터 REST API 컨트롤러.
 *
 * <p>엔드포인트:
 *   GET /api/tickers/latest              — 전체 심볼 최신 시세 1건씩 (10건)
 *   GET /api/tickers/{symbol}/history    — 특정 심볼 최근 N건 (기본 100, 최대 1000)
 *
 * <p>사용 예시:
 *   curl http://localhost:8080/api/tickers/latest
 *   curl http://localhost:8080/api/tickers/BTCUSDT/history?limit=50
 */
@RestController
@RequestMapping("/api/tickers")
public class TickerController {

    private final TickerService tickerService;

    public TickerController(TickerService tickerService) {
        this.tickerService = tickerService;
    }

    /**
     * 현재 구독 중인 모든 심볼(10개)의 최신 시세를 반환한다.
     * React 대시보드 초기 렌더링 및 WebSocket 연결 전 첫 데이터 로딩에 사용한다.
     */
    @GetMapping("/latest")
    public List<Ticker> getLatest() {
        return tickerService.getLatestAll();
    }

    /**
     * 특정 심볼의 시세 이력을 최신순으로 반환한다.
     * 차트 데이터 로딩에 사용한다.
     *
     * @param symbol 심볼 (대소문자 무관, 예: btcusdt, BTCUSDT)
     * @param limit  최대 반환 건수 (기본값 100)
     */
    @GetMapping("/{symbol}/history")
    public List<Ticker> getHistory(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "100") int limit) {
        return tickerService.getHistory(symbol, limit);
    }
}
