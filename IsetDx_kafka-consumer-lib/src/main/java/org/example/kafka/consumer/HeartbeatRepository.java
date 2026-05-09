package org.example.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * isetdx_kafka DB의 6개 테이블에 데이터를 적재한다.
 *
 * <p>단일 JDBC Connection을 유지하며 끊어진 경우 자동 재연결한다.
 * 다중 스레드에서 호출되므로 모든 public 메서드는 synchronized로 보호한다.
 */
class HeartbeatRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatRepository.class);

    private final String url;
    private final String user;
    private final String password;

    private Connection conn;

    HeartbeatRepository(ConsumerSettings settings) {
        this.url      = settings.dbUrl();
        this.user     = settings.dbUser();
        this.password = settings.dbPassword();
    }

    // ── producer_heartbeat_realtime ──────────────────────────────────────────

    synchronized void upsertRealtime(HeartbeatEvent e, long intervalMs, String state) {
        String sql = "INSERT INTO producer_heartbeat_realtime " +
                     "(instance_id, weight, state, received_at, interval_ms) VALUES (?, ?, ?, ?, ?) " +
                     "ON DUPLICATE KEY UPDATE " +
                     "weight = VALUES(weight), state = VALUES(state), " +
                     "received_at = VALUES(received_at), " +
                     "interval_ms = VALUES(interval_ms), updated_at = CURRENT_TIMESTAMP(3)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, e.getInstanceId());
            ps.setInt(2, e.getWeight());
            ps.setString(3, state);
            ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            ps.setObject(5, intervalMs < 0 ? null : intervalMs);
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("upsertRealtime 실패: {}", ex.getMessage());
            resetConnection();
        }
    }

    synchronized void updateRealtimeState(String instanceId, String state) {
        String sql = "UPDATE producer_heartbeat_realtime SET state = ?, updated_at = CURRENT_TIMESTAMP(3) " +
                     "WHERE instance_id = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, state);
            ps.setString(2, instanceId);
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("updateRealtimeState 실패: {}", ex.getMessage());
            resetConnection();
        }
    }

    // ── producer_heartbeat_history ───────────────────────────────────────────

    synchronized void insertHistory(String instanceId, int weight, String state) {
        String sql = "INSERT INTO producer_heartbeat_history " +
                     "(instance_id, weight, state, received_at) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.setInt(2, weight);
            ps.setString(3, state);
            ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("insertHistory 실패: {}", ex.getMessage());
            resetConnection();
        }
    }

    // ── producer_status_log ──────────────────────────────────────────────────

    synchronized void insertStatusLog(StatusEvent e) {
        String sql = "INSERT INTO producer_status_log " +
                     "(instance_id, weight, state, event_at) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, e.getInstanceId());
            ps.setInt(2, e.getWeight());
            ps.setString(3, e.getState());
            ps.setTimestamp(4, new Timestamp(e.getTimestamp() > 0 ? e.getTimestamp() : System.currentTimeMillis()));
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("insertStatusLog 실패: {}", ex.getMessage());
            resetConnection();
        }
    }

    // ── failover_event_log ───────────────────────────────────────────────────

    synchronized void insertFailover(String instanceId, String fromState, String toState,
                                     String reason, long recoveryMs) {
        String sql = "INSERT INTO failover_event_log " +
                     "(instance_id, from_state, to_state, reason, failover_at, recovery_ms) " +
                     "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.setString(2, fromState);
            ps.setString(3, toState);
            ps.setString(4, reason);
            ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            ps.setObject(6, recoveryMs < 0 ? null : recoveryMs);
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("insertFailover 실패: {}", ex.getMessage());
            resetConnection();
        }
    }

    // ── topic_throughput_stats ───────────────────────────────────────────────

    synchronized void insertThroughput(String topic, String consumerGroup,
                                       long messageCount, long windowStart, long windowEnd) {
        String sql = "INSERT INTO topic_throughput_stats " +
                     "(topic, consumer_group, message_count, window_start, window_end) " +
                     "VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setString(2, consumerGroup);
            ps.setLong(3, messageCount);
            ps.setTimestamp(4, new Timestamp(windowStart));
            ps.setTimestamp(5, new Timestamp(windowEnd));
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("insertThroughput 실패: {}", ex.getMessage());
            resetConnection();
        }
    }

    // ── consumer_error_log ───────────────────────────────────────────────────

    synchronized void insertError(String topic, String consumerGroup,
                                  String errorType, String errorMessage, String rawPayload) {
        String sql = "INSERT INTO consumer_error_log " +
                     "(topic, consumer_group, error_type, error_message, raw_payload) " +
                     "VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setString(2, consumerGroup);
            ps.setString(3, errorType);
            ps.setString(4, errorMessage);
            ps.setString(5, rawPayload);
            ps.executeUpdate();
        } catch (SQLException ex) {
            log.error("insertError 실패: {}", ex.getMessage());
        }
    }

    // ── 시작 시 상태 초기화 조회 ─────────────────────────────────────────────

    /**
     * 인스턴스별 마지막 상태를 DB에서 읽어 반환한다.
     * HeartbeatMonitor 시작 시 instanceState 맵을 초기화하는 데 사용한다.
     */
    synchronized Map<String, String> loadLastStates() {
        String sql = "SELECT instance_id, state FROM producer_status_log " +
                     "WHERE id IN (SELECT MAX(id) FROM producer_status_log GROUP BY instance_id)";
        Map<String, String> result = new HashMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.put(rs.getString("instance_id"), rs.getString("state"));
            }
            log.info("instanceState 초기화: {}건 로드", result.size());
        } catch (SQLException e) {
            log.error("loadLastStates 실패: {}", e.getMessage());
            resetConnection();
        }
        return result;
    }

    /**
     * 마지막 failover 이벤트가 ACTIVE→STANDBY 인 인스턴스의 전환 시각을 반환한다.
     * HeartbeatMonitor 시작 시 failoverOutTime 맵을 초기화하는 데 사용한다.
     * 이미 STANDBY→ACTIVE 로 회복된 인스턴스는 포함되지 않는다.
     */
    synchronized Map<String, Long> loadFailoverOutTimes() {
        String sql = "SELECT instance_id, failover_at FROM failover_event_log f1 " +
                     "WHERE to_state = 'STANDBY' " +
                     "AND id = (SELECT MAX(id) FROM failover_event_log f2 WHERE f2.instance_id = f1.instance_id)";
        Map<String, Long> result = new HashMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("failover_at");
                result.put(rs.getString("instance_id"), ts.getTime());
            }
            log.info("failoverOutTime 초기화: {}건 로드", result.size());
        } catch (SQLException e) {
            log.error("loadFailoverOutTimes 실패: {}", e.getMessage());
            resetConnection();
        }
        return result;
    }

    // ── 연결 관리 ────────────────────────────────────────────────────────────

    private Connection getConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = DriverManager.getConnection(url, user, password);
            log.info("DB 연결 수립: {}", url);
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
