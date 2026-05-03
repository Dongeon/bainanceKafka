package org.example.repository;

import org.example.model.Ticker;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

/**
 * ticker 테이블에 대한 JPA Repository.
 * 읽기 전용으로만 사용한다.
 */
public interface TickerRepository extends JpaRepository<Ticker, Long> {

    /**
     * 10개 심볼 각각의 가장 최신 레코드를 한 번에 조회한다.
     *
     * <p>Native SQL로 구현한 이유:
     * JPQL의 서브쿼리 방식(correlated subquery)은 심볼마다 MAX 스캔을 반복해 N+1이 발생한다.
     * 이 방식은 GROUP BY → JOIN으로 한 번에 처리하므로 idx_symbol_event_time 인덱스를 효율적으로 사용한다.
     */
    @Query(value = """
            SELECT t.* FROM ticker t
            INNER JOIN (
                SELECT symbol, MAX(event_time) AS max_time
                FROM ticker
                GROUP BY symbol
            ) latest ON t.symbol = latest.symbol AND t.event_time = latest.max_time
            """, nativeQuery = true)
    List<Ticker> findLatestPerSymbol();

    /**
     * 특정 심볼의 시세 이력을 최신순으로 조회한다.
     * Pageable로 limit을 제어한다. 예: PageRequest.of(0, 100)
     *
     * @param symbol   조회할 심볼 (예: "BTCUSDT")
     * @param pageable 페이지 설정 (size = 반환할 최대 건수)
     */
    List<Ticker> findBySymbolOrderByEventTimeDesc(String symbol, Pageable pageable);
}
