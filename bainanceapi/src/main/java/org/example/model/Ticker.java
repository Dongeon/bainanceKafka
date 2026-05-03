package org.example.model;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * bainance.ticker 테이블에 매핑되는 JPA 엔티티.
 *
 * <p>이 테이블은 bainanceconsumer가 Kafka에서 읽어 저장한 바이낸스 실시간 시세 데이터다.
 * 이 애플리케이션은 읽기 전용으로만 사용하며 (ddl-auto=none), 직접 쓰지 않는다.
 *
 * <p>컬럼명 매핑:
 * Spring의 SpringPhysicalNamingStrategy가 camelCase를 snake_case로 자동 변환한다.
 * 예: lastPrice → last_price, priceChangePct → price_change_pct
 */
@Entity
@Table(name = "ticker")
public class Ticker {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 코인 심볼 (예: BTCUSDT) */
    private String symbol;

    /** 이벤트 발생 시각 KST. Producer에서 변환하여 저장된 값. */
    private LocalDateTime eventTime;

    // ── 가격 ────────────────────────────────────────────────────────────────
    private BigDecimal lastPrice;         // 현재가
    private BigDecimal openPrice;         // 24h 시가
    private BigDecimal highPrice;         // 24h 고가
    private BigDecimal lowPrice;          // 24h 저가
    private BigDecimal prevClosePrice;    // 전일 종가
    private BigDecimal weightedAvgPrice;  // 24h 가중평균가

    // ── 변동 ────────────────────────────────────────────────────────────────
    private BigDecimal priceChange;       // 24h 가격 변동 (절댓값, USDT)
    private BigDecimal priceChangePct;    // 24h 변동률 (%)

    // ── 호가 ────────────────────────────────────────────────────────────────
    private BigDecimal bidPrice;          // 최우선 매수 호가
    private BigDecimal bidQty;            // 최우선 매수 수량
    private BigDecimal askPrice;          // 최우선 매도 호가
    private BigDecimal askQty;            // 최우선 매도 수량
    private BigDecimal lastQty;           // 마지막 체결 수량

    // ── 거래량 ──────────────────────────────────────────────────────────────
    private BigDecimal baseVolume;        // 코인 기준 거래량
    private BigDecimal quoteVolume;       // USDT 기준 거래량 (거래대금)
    private Integer tradeCount;           // 24h 체결 횟수

    private LocalDateTime createdAt;

    // ── Getters ─────────────────────────────────────────────────────────────
    public Long getId()                    { return id; }
    public String getSymbol()              { return symbol; }
    public LocalDateTime getEventTime()    { return eventTime; }
    public BigDecimal getLastPrice()       { return lastPrice; }
    public BigDecimal getOpenPrice()       { return openPrice; }
    public BigDecimal getHighPrice()       { return highPrice; }
    public BigDecimal getLowPrice()        { return lowPrice; }
    public BigDecimal getPrevClosePrice()  { return prevClosePrice; }
    public BigDecimal getWeightedAvgPrice(){ return weightedAvgPrice; }
    public BigDecimal getPriceChange()     { return priceChange; }
    public BigDecimal getPriceChangePct()  { return priceChangePct; }
    public BigDecimal getBidPrice()        { return bidPrice; }
    public BigDecimal getBidQty()          { return bidQty; }
    public BigDecimal getAskPrice()        { return askPrice; }
    public BigDecimal getAskQty()          { return askQty; }
    public BigDecimal getLastQty()         { return lastQty; }
    public BigDecimal getBaseVolume()      { return baseVolume; }
    public BigDecimal getQuoteVolume()     { return quoteVolume; }
    public Integer getTradeCount()         { return tradeCount; }
    public LocalDateTime getCreatedAt()    { return createdAt; }
}
