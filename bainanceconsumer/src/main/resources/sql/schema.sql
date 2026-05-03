-- ============================================================
-- Database
-- ============================================================
CREATE DATABASE IF NOT EXISTS bainance
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE bainance;

-- ============================================================
-- kline 테이블 (캔들스틱 차트용 — 확정 캔들만 저장)
-- ============================================================
CREATE TABLE IF NOT EXISTS kline (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    symbol          VARCHAR(20)     NOT NULL COMMENT '코인 심볼 (BTCUSDT)',
    interval_type   VARCHAR(5)      NOT NULL COMMENT '캔들 인터벌 (1m, 5m, 15m, 1h)',
    open_time       DATETIME(3)     NOT NULL COMMENT '캔들 시작 시각 (KST)',
    close_time      DATETIME(3)     NOT NULL COMMENT '캔들 종료 시각 (KST)',

    open_price      DECIMAL(30, 8)  NOT NULL COMMENT '시가',
    high_price      DECIMAL(30, 8)  NOT NULL COMMENT '고가',
    low_price       DECIMAL(30, 8)  NOT NULL COMMENT '저가',
    close_price     DECIMAL(30, 8)  NOT NULL COMMENT '종가',

    volume          DECIMAL(40, 8)  NOT NULL COMMENT '코인 기준 거래량',
    quote_volume    DECIMAL(40, 8)  NOT NULL COMMENT 'USDT 기준 거래량',
    trade_count     INT             NOT NULL COMMENT '체결 횟수',

    created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (id),
    UNIQUE KEY uq_symbol_interval_opentime (symbol, interval_type, open_time),
    INDEX idx_symbol_interval_opentime (symbol, interval_type, open_time DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='바이낸스 확정 캔들 데이터 (is_closed=true 만 저장)';

-- ============================================================
-- ticker 테이블
-- ============================================================
CREATE TABLE IF NOT EXISTS ticker (
    id                  BIGINT          NOT NULL AUTO_INCREMENT,
    symbol              VARCHAR(20)     NOT NULL COMMENT '코인 심볼 (BTCUSDT)',
    event_time          DATETIME(3)     NOT NULL COMMENT '이벤트 발생 시각 (KST)',

    -- 가격
    last_price          DECIMAL(30, 8)  NOT NULL COMMENT '현재가',
    open_price          DECIMAL(30, 8)  NOT NULL COMMENT '24h 시가',
    high_price          DECIMAL(30, 8)  NOT NULL COMMENT '24h 고가',
    low_price           DECIMAL(30, 8)  NOT NULL COMMENT '24h 저가',
    prev_close_price    DECIMAL(30, 8)  NOT NULL COMMENT '전일 종가',
    weighted_avg_price  DECIMAL(30, 8)  NOT NULL COMMENT '24h 가중평균가',

    -- 변동
    price_change        DECIMAL(30, 8)  NOT NULL COMMENT '24h 가격 변동 (절댓값)',
    price_change_pct    DECIMAL(10, 4)  NOT NULL COMMENT '24h 변동률 (%)',

    -- 호가
    bid_price           DECIMAL(30, 8)  NOT NULL COMMENT '최우선 매수 호가',
    bid_qty             DECIMAL(30, 8)  NOT NULL COMMENT '최우선 매수 수량',
    ask_price           DECIMAL(30, 8)  NOT NULL COMMENT '최우선 매도 호가',
    ask_qty             DECIMAL(30, 8)  NOT NULL COMMENT '최우선 매도 수량',
    last_qty            DECIMAL(30, 8)  NOT NULL COMMENT '마지막 체결 수량',

    -- 거래량
    base_volume         DECIMAL(40, 8)  NOT NULL COMMENT '코인 기준 거래량',
    quote_volume        DECIMAL(40, 8)  NOT NULL COMMENT 'USDT 기준 거래량',
    trade_count         INT             NOT NULL COMMENT '24h 체결 횟수',

    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (id),
    INDEX idx_symbol_event_time (symbol, event_time DESC)   COMMENT '심볼별 시계열 조회'
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='바이낸스 실시간 ticker 데이터';
