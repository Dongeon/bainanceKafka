package org.example.repository;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.model.TickerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class TickerRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TickerRepository.class);

    private static final String INSERT_SQL =
            "INSERT INTO ticker (" +
            "  symbol, event_time, last_price, open_price, high_price, low_price," +
            "  prev_close_price, weighted_avg_price, price_change, price_change_pct," +
            "  bid_price, bid_qty, ask_price, ask_qty, last_qty," +
            "  base_volume, quote_volume, trade_count" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final HikariDataSource dataSource;

    public TickerRepository(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(3000);
        config.setPoolName("ticker-pool");
        this.dataSource = new HikariDataSource(config);
        log.info("Ticker DB 연결 풀 초기화: {}", jdbcUrl);
    }

    public void save(TickerEvent e) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

            ps.setString(1,     e.getSymbol());
            ps.setString(2,     e.getEventTimeKst());
            ps.setBigDecimal(3,  toBigDecimal(e.getLastPrice()));
            ps.setBigDecimal(4,  toBigDecimal(e.getOpenPrice()));
            ps.setBigDecimal(5,  toBigDecimal(e.getHighPrice()));
            ps.setBigDecimal(6,  toBigDecimal(e.getLowPrice()));
            ps.setBigDecimal(7,  toBigDecimal(e.getPrevClosePrice()));
            ps.setBigDecimal(8,  toBigDecimal(e.getWeightedAvgPrice()));
            ps.setBigDecimal(9,  toBigDecimal(e.getPriceChange()));
            ps.setBigDecimal(10, toBigDecimal(e.getPriceChangePercent()));
            ps.setBigDecimal(11, toBigDecimal(e.getBidPrice()));
            ps.setBigDecimal(12, toBigDecimal(e.getBidQty()));
            ps.setBigDecimal(13, toBigDecimal(e.getAskPrice()));
            ps.setBigDecimal(14, toBigDecimal(e.getAskQty()));
            ps.setBigDecimal(15, toBigDecimal(e.getLastQty()));
            ps.setBigDecimal(16, toBigDecimal(e.getBaseVolume()));
            ps.setBigDecimal(17, toBigDecimal(e.getQuoteVolume()));
            ps.setLong(18, e.getTradeCount());

            ps.executeUpdate();
        } catch (Exception ex) {
            log.error("Ticker DB 저장 실패 [{}]: {}", e.getSymbol(), ex.getMessage());
        }
    }

    private BigDecimal toBigDecimal(String value) {
        if (value == null || value.isBlank()) return BigDecimal.ZERO;
        return new BigDecimal(value);
    }

    @Override
    public void close() {
        dataSource.close();
        log.info("Ticker DB 연결 풀 종료");
    }
}
