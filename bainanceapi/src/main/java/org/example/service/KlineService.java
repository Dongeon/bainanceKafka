package org.example.service;

import org.example.model.Kline;
import org.example.repository.KlineRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional(readOnly = true)
public class KlineService {

    private final KlineRepository klineRepository;

    public KlineService(KlineRepository klineRepository) {
        this.klineRepository = klineRepository;
    }

    /**
     * 특정 심볼+인터벌의 캔들 이력을 최신순으로 반환한다.
     *
     * @param symbol   심볼 (예: "BTCUSDT")
     * @param interval 인터벌 (예: "1m", "5m", "15m", "1h")
     * @param limit    최대 반환 건수 (최대 1000)
     */
    public List<Kline> getKlines(String symbol, String interval, int limit) {
        int safeLimit = Math.min(limit, 1000);
        return klineRepository.findBySymbolAndIntervalTypeOrderByOpenTimeDesc(
                symbol.toUpperCase(), interval, PageRequest.of(0, safeLimit));
    }
}
