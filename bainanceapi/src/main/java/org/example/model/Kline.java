package org.example.model;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * bainance.kline 테이블에 매핑되는 JPA 엔티티.
 * bainanceconsumer가 저장한 확정 캔들 데이터를 읽기 전용으로 사용한다.
 */
@Entity
@Table(name = "kline")
public class Kline {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String symbol;
    private String intervalType;   // 1m, 5m, 15m, 1h
    private LocalDateTime openTime;
    private LocalDateTime closeTime;

    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;
    private BigDecimal volume;
    private BigDecimal quoteVolume;
    private Integer tradeCount;

    private LocalDateTime createdAt;

    public Long          getId()          { return id; }
    public String        getSymbol()      { return symbol; }
    public String        getIntervalType(){ return intervalType; }
    public LocalDateTime getOpenTime()    { return openTime; }
    public LocalDateTime getCloseTime()   { return closeTime; }
    public BigDecimal    getOpenPrice()   { return openPrice; }
    public BigDecimal    getHighPrice()   { return highPrice; }
    public BigDecimal    getLowPrice()    { return lowPrice; }
    public BigDecimal    getClosePrice()  { return closePrice; }
    public BigDecimal    getVolume()      { return volume; }
    public BigDecimal    getQuoteVolume() { return quoteVolume; }
    public Integer       getTradeCount()  { return tradeCount; }
    public LocalDateTime getCreatedAt()   { return createdAt; }
}
