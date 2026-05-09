-- ============================================================
-- IsetDx Kafka Consumer — DB 초기화 스크립트
-- 실행: docker exec mariadb_bainance mariadb -u root -p < schema.sql
-- ============================================================

-- ── 데이터베이스 ──────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS isetdx_kafka
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- ── 계정 생성 및 권한 ──────────────────────────────────────────
CREATE USER IF NOT EXISTS 'isetdx_kafka'@'%'     IDENTIFIED BY 'isetdx1234';
CREATE USER IF NOT EXISTS 'isetdx_kafka'@'localhost' IDENTIFIED BY 'isetdx1234';

GRANT ALL PRIVILEGES ON isetdx_kafka.* TO 'isetdx_kafka'@'%';
GRANT ALL PRIVILEGES ON isetdx_kafka.* TO 'isetdx_kafka'@'localhost';
FLUSH PRIVILEGES;

USE isetdx_kafka;

-- ============================================================
-- 1. producer_heartbeat_realtime
--    인스턴스별 최신 heartbeat 1건 (UPSERT)
-- ============================================================
CREATE TABLE IF NOT EXISTS producer_heartbeat_realtime (
    instance_id     VARCHAR(100)    NOT NULL,
    weight          INT             NOT NULL COMMENT '선출 우선순위',
    received_at     DATETIME(3)     NOT NULL COMMENT '마지막 heartbeat 수신 시각',
    interval_ms     BIGINT          COMMENT '직전 heartbeat와의 간격 (ms)',
    updated_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                    ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (instance_id)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='인스턴스별 최신 heartbeat 상태 (실시간 조회용)';

-- ============================================================
-- 2. producer_heartbeat_history
--    heartbeat 수신 이력 (시계열, INSERT only)
-- ============================================================
CREATE TABLE IF NOT EXISTS producer_heartbeat_history (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    instance_id     VARCHAR(100)    NOT NULL,
    weight          INT             NOT NULL,
    received_at     DATETIME(3)     NOT NULL COMMENT 'heartbeat 수신 시각',
    interval_ms     BIGINT          COMMENT '직전 heartbeat와의 간격 (ms)',

    PRIMARY KEY (id),
    INDEX idx_instance_received (instance_id, received_at DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='heartbeat 수신 전체 이력';

-- ============================================================
-- 3. producer_status_log
--    PREPARING / ACTIVE / STANDBY / OFFLINE 전환 이력
-- ============================================================
CREATE TABLE IF NOT EXISTS producer_status_log (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    instance_id     VARCHAR(100)    NOT NULL,
    weight          INT             NOT NULL,
    state           VARCHAR(20)     NOT NULL COMMENT 'PREPARING|ACTIVE|STANDBY|OFFLINE',
    event_at        DATETIME(3)     NOT NULL COMMENT '이벤트 발생 시각 (producer 측)',
    recorded_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                    COMMENT 'consumer 수신 시각',

    PRIMARY KEY (id),
    INDEX idx_instance_event (instance_id, event_at DESC),
    INDEX idx_state_event (state, event_at DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='producer 상태 전환 이력';

-- ============================================================
-- 4. failover_event_log
--    ACTIVE ↔ STANDBY 전환 이력 + 복구 소요 시간
-- ============================================================
CREATE TABLE IF NOT EXISTS failover_event_log (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    instance_id     VARCHAR(100)    NOT NULL,
    from_state      VARCHAR(20)     NOT NULL,
    to_state        VARCHAR(20)     NOT NULL,
    reason          VARCHAR(50)     COMMENT 'TIMEOUT|SPLIT_BRAIN|OFFLINE_DETECTED|PREEMPTED',
    failover_at     DATETIME(3)     NOT NULL COMMENT '전환 발생 시각',
    recovery_ms     BIGINT          COMMENT 'STANDBY→ACTIVE 복구까지 걸린 시간 (ms)',

    PRIMARY KEY (id),
    INDEX idx_instance_failover (instance_id, failover_at DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='failover 전환 이력 및 복구 소요 시간';

-- ============================================================
-- 5. topic_throughput_stats
--    토픽별 N초 단위 메시지 수 집계
-- ============================================================
CREATE TABLE IF NOT EXISTS topic_throughput_stats (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    topic           VARCHAR(100)    NOT NULL,
    consumer_group  VARCHAR(100)    NOT NULL,
    message_count   BIGINT          NOT NULL COMMENT '집계 구간 내 처리 메시지 수',
    window_start    DATETIME(3)     NOT NULL COMMENT '집계 시작 시각',
    window_end      DATETIME(3)     NOT NULL COMMENT '집계 종료 시각',

    PRIMARY KEY (id),
    INDEX idx_topic_window (topic, window_start DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='토픽별 처리량 집계 (시계열)';

-- ============================================================
-- 6. consumer_error_log
--    파싱 / DB 저장 실패 이력
-- ============================================================
CREATE TABLE IF NOT EXISTS consumer_error_log (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    topic           VARCHAR(100)    NOT NULL,
    consumer_group  VARCHAR(100),
    error_type      VARCHAR(50)     NOT NULL COMMENT 'PARSE_ERROR|DB_ERROR|UNKNOWN',
    error_message   TEXT,
    raw_payload     TEXT            COMMENT '실패한 원본 메시지',
    occurred_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (id),
    INDEX idx_topic_occurred (topic, occurred_at DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='consumer 처리 오류 이력';
