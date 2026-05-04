package org.example.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Kafka crypto-ticker 토픽 메시지를 역직렬화하여 WebSocket으로 브로드캐스트하는 DTO.
 *
 * <p>Kafka JSON은 바이낸스 원본 약어 필드명(c, s, E 등)과 eventTimeKst를 포함한다.
 * @JsonAlias로 약어를 매핑하고, 직렬화(프론트 전송)는 Java 필드명(camelCase)을 그대로 사용한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TickerDto {

    @JsonAlias("s")            private String symbol;
    @JsonAlias("eventTimeKst") private String eventTime;
    @JsonAlias("c")            private String lastPrice;
    @JsonAlias("o")            private String openPrice;
    @JsonAlias("h")            private String highPrice;
    @JsonAlias("l")            private String lowPrice;
    @JsonAlias("x")            private String prevClosePrice;
    @JsonAlias("w")            private String weightedAvgPrice;
    @JsonAlias("p")            private String priceChange;
    @JsonAlias("P")            private String priceChangePct;
    @JsonAlias("b")            private String bidPrice;
    @JsonAlias("B")            private String bidQty;
    @JsonAlias("a")            private String askPrice;
    @JsonAlias("A")            private String askQty;
    @JsonAlias("Q")            private String lastQty;
    @JsonAlias("v")            private String baseVolume;
    @JsonAlias("q")            private String quoteVolume;
    @JsonAlias("n")            private long   tradeCount;

    public String getSymbol()           { return symbol; }
    public String getEventTime()        { return eventTime; }
    public String getLastPrice()        { return lastPrice; }
    public String getOpenPrice()        { return openPrice; }
    public String getHighPrice()        { return highPrice; }
    public String getLowPrice()         { return lowPrice; }
    public String getPrevClosePrice()   { return prevClosePrice; }
    public String getWeightedAvgPrice() { return weightedAvgPrice; }
    public String getPriceChange()      { return priceChange; }
    public String getPriceChangePct()   { return priceChangePct; }
    public String getBidPrice()         { return bidPrice; }
    public String getBidQty()           { return bidQty; }
    public String getAskPrice()         { return askPrice; }
    public String getAskQty()           { return askQty; }
    public String getLastQty()          { return lastQty; }
    public String getBaseVolume()       { return baseVolume; }
    public String getQuoteVolume()      { return quoteVolume; }
    public long   getTradeCount()       { return tradeCount; }
}
