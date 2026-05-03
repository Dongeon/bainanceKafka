package org.example.controller;

import org.example.model.Kline;
import org.example.service.KlineService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 캔들스틱 차트 데이터 REST API 컨트롤러.
 *
 * <p>엔드포인트:
 *   GET /api/klines/{symbol}?interval=1m&limit=200
 *
 * <p>사용 예시:
 *   curl "http://localhost:8080/api/klines/BTCUSDT?interval=1m&limit=200"
 *   curl "http://localhost:8080/api/klines/ETHUSDT?interval=1h&limit=100"
 */
@RestController
@RequestMapping("/api/klines")
public class KlineController {

    private final KlineService klineService;

    public KlineController(KlineService klineService) {
        this.klineService = klineService;
    }

    /**
     * 특정 심볼의 캔들 이력을 최신순으로 반환한다.
     *
     * @param symbol   심볼 (대소문자 무관, 예: btcusdt, BTCUSDT)
     * @param interval 인터벌 (1m, 5m, 15m, 1h). 기본값 1m
     * @param limit    최대 반환 건수. 기본값 200, 최대 1000
     */
    @GetMapping("/{symbol}")
    public List<Kline> getKlines(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "1m") String interval,
            @RequestParam(defaultValue = "200") int limit) {
        return klineService.getKlines(symbol, interval, limit);
    }
}
