package org.example.repository;

import org.example.model.Kline;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface KlineRepository extends JpaRepository<Kline, Long> {

    List<Kline> findBySymbolAndIntervalTypeOrderByOpenTimeDesc(
            String symbol, String intervalType, Pageable pageable);
}
