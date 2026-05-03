package org.example.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.model.KlineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class KlineRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KlineRepository.class);

    private static final String INSERT_SQL = """
            INSERT INTO kline (
                symbol, interval_type, open_time, close_time,
                open_price, high_price, low_price, close_price,
                volume, quote_volume, trade_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                open_price   = VALUES(open_price),
                high_price   = VALUES(high_price),
                low_price    = VALUES(low_price),
                close_price  = VALUES(close_price),
                volume       = VALUES(volume),
                quote_volume = VALUES(quote_volume),
                trade_count  = VALUES(trade_count)
            """;

    private final HikariDataSource dataSource;

    public KlineRepository(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(3000);
        config.setPoolName("kline-pool");
        this.dataSource = new HikariDataSource(config);
        log.info("Kline DB 연결 풀 초기화 완료");
    }

    public void save(KlineEvent e) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

            ps.setString(1,     e.getSymbol());
            ps.setString(2,     e.getInterval());
            ps.setString(3,     e.getOpenTimeKst());
            ps.setString(4,     e.getCloseTimeKst());
            ps.setBigDecimal(5, toBigDecimal(e.getOpenPrice()));
            ps.setBigDecimal(6, toBigDecimal(e.getHighPrice()));
            ps.setBigDecimal(7, toBigDecimal(e.getLowPrice()));
            ps.setBigDecimal(8, toBigDecimal(e.getClosePrice()));
            ps.setBigDecimal(9, toBigDecimal(e.getVolume()));
            ps.setBigDecimal(10, toBigDecimal(e.getQuoteVolume()));
            ps.setInt(11, e.getTradeCount());

            ps.executeUpdate();

        } catch (Exception ex) {
            log.error("Kline DB 저장 실패 [{}@{}]: {}", e.getSymbol(), e.getInterval(), ex.getMessage());
        }
    }

    private BigDecimal toBigDecimal(String value) {
        if (value == null || value.isBlank()) return BigDecimal.ZERO;
        return new BigDecimal(value);
    }

    @Override
    public void close() {
        dataSource.close();
        log.info("Kline DB 연결 풀 종료");
    }
}
