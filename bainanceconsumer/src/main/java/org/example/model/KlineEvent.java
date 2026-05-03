package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kafka crypto-kline 토픽에서 소비하는 확정 캔들 이벤트.
 * Producer가 직렬화한 플랫 JSON 구조와 1:1로 대응한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KlineEvent {

    @JsonProperty("symbol")       private String symbol;
    @JsonProperty("interval")     private String interval;
    @JsonProperty("openTimeKst")  private String openTimeKst;
    @JsonProperty("closeTimeKst") private String closeTimeKst;
    @JsonProperty("openPrice")    private String openPrice;
    @JsonProperty("highPrice")    private String highPrice;
    @JsonProperty("lowPrice")     private String lowPrice;
    @JsonProperty("closePrice")   private String closePrice;
    @JsonProperty("volume")       private String volume;
    @JsonProperty("quoteVolume")  private String quoteVolume;
    @JsonProperty("tradeCount")   private int    tradeCount;

    public String getSymbol()       { return symbol; }
    public String getInterval()     { return interval; }
    public String getOpenTimeKst()  { return openTimeKst; }
    public String getCloseTimeKst() { return closeTimeKst; }
    public String getOpenPrice()    { return openPrice; }
    public String getHighPrice()    { return highPrice; }
    public String getLowPrice()     { return lowPrice; }
    public String getClosePrice()   { return closePrice; }
    public String getVolume()       { return volume; }
    public String getQuoteVolume()  { return quoteVolume; }
    public int    getTradeCount()   { return tradeCount; }
}
