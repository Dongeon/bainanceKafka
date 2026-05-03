package org.example.service;

import org.example.model.Ticker;
import org.example.repository.TickerRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 시세 데이터 조회 서비스.
 * Controller와 BroadcastService 양쪽에서 사용한다.
 */
@Service
@Transactional(readOnly = true)
public class TickerService {

    private final TickerRepository tickerRepository;

    public TickerService(TickerRepository tickerRepository) {
        this.tickerRepository = tickerRepository;
    }

    /**
     * 구독 중인 모든 심볼(10개)의 최신 시세를 반환한다.
     * WebSocket 브로드캐스트와 REST /api/tickers/latest에서 사용한다.
     */
    public List<Ticker> getLatestAll() {
        return tickerRepository.findLatestPerSymbol();
    }

    /**
     * 특정 심볼의 최근 시세 이력을 최신순으로 반환한다.
     *
     * @param symbol 심볼 (예: "BTCUSDT")
     * @param limit  최대 반환 건수 (기본값 100, 최대 1000)
     */
    public List<Ticker> getHistory(String symbol, int limit) {
        int safeLimit = Math.min(limit, 1000); // 과도한 조회 방지
        return tickerRepository.findBySymbolOrderByEventTimeDesc(
                symbol.toUpperCase(),
                PageRequest.of(0, safeLimit)
        );
    }
}
