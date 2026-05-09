package org.example.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * topic_production_stats, consumer_group_lag 테이블에 데이터를 적재한다.
 */
class TopicMonitorRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicMonitorRepository.class);

    private final String url;
    private final String user;
    private final String password;

    private Connection conn;

    TopicMonitorRepository(ConsumerSettings settings) {
        this.url      = settings.dbUrl();
        this.user     = settings.dbUser();
        this.password = settings.dbPassword();
    }

    // ── topic_production_stats ───────────────────────────────────────────────

    synchronized void insertProductionStats(String topic, int partitionCount,
                                            long totalOffset, long messageDelta) {
        String sql = "INSERT INTO topic_production_stats " +
                     "(topic, partition_count, total_offset, message_delta) " +
                     "VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partitionCount);
            ps.setLong(3, totalOffset);
            ps.setLong(4, messageDelta);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("insertProductionStats 실패 [{}]: {}", topic, e.getMessage());
            resetConnection();
        }
    }

    // ── consumer_group_lag ───────────────────────────────────────────────────

    synchronized void insertConsumerGroupLag(String consumerGroup, String topic, long totalLag) {
        String sql = "INSERT INTO consumer_group_lag " +
                     "(consumer_group, topic, total_lag) VALUES (?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, consumerGroup);
            ps.setString(2, topic);
            ps.setLong(3, totalLag);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("insertConsumerGroupLag 실패 [{}/{}]: {}", consumerGroup, topic, e.getMessage());
            resetConnection();
        }
    }

    // ── 연결 관리 ────────────────────────────────────────────────────────────

    private Connection getConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = DriverManager.getConnection(url, user, password);
            log.info("TopicMonitor DB 연결 수립: {}", url);
        }
        return conn;
    }

    private void resetConnection() {
        try {
            if (conn != null && !conn.isClosed()) conn.close();
        } catch (SQLException ignored) {}
        conn = null;
    }

    @Override
    public synchronized void close() {
        resetConnection();
    }
}
