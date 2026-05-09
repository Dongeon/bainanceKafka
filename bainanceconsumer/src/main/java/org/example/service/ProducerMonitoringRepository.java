package org.example.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class ProducerMonitoringRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProducerMonitoringRepository.class);

    private static final String UPSERT_SQL =
            "INSERT INTO producer_monitoring (instance_id, weight, status) VALUES (?, ?, ?)" +
            " ON DUPLICATE KEY UPDATE weight = VALUES(weight), status = VALUES(status), updated_at = current_timestamp(3)";

    private static final String INSERT_HIS_SQL =
            "INSERT INTO producer_monitoring_his (instance_id, weight, status) VALUES (?, ?, ?)";

    private final HikariDataSource ds;

    public ProducerMonitoringRepository(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(5000);
        this.ds = new HikariDataSource(config);
        log.info("ProducerMonitoringRepository 초기화 완료");
    }

    public void upsertStatus(String instanceId, int weight, String status) {
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
                ps.setString(1, instanceId);
                ps.setInt(2, weight);
                ps.setString(3, status);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(INSERT_HIS_SQL)) {
                ps.setString(1, instanceId);
                ps.setInt(2, weight);
                ps.setString(3, status);
                ps.executeUpdate();
            }
            log.info("[MONITORING] {} weight={} status={}", instanceId, weight, status);
        } catch (Exception e) {
            log.error("[MONITORING] 저장 실패 [{} {}]: {}", instanceId, status, e.getMessage());
        }
    }

    @Override
    public void close() {
        ds.close();
    }
}
