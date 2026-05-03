package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TickerEvent {

    @JsonProperty("E") private long   eventTime;
    @JsonProperty("s") private String symbol;
    @JsonProperty("c") private String lastPrice;
    @JsonProperty("o") private String openPrice;
    @JsonProperty("h") private String highPrice;
    @JsonProperty("l") private String lowPrice;
    @JsonProperty("x") private String prevClosePrice;
    @JsonProperty("w") private String weightedAvgPrice;
    @JsonProperty("p") private String priceChange;
    @JsonProperty("P") private String priceChangePercent;
    @JsonProperty("b") private String bidPrice;
    @JsonProperty("B") private String bidQty;
    @JsonProperty("a") private String askPrice;
    @JsonProperty("A") private String askQty;
    @JsonProperty("Q") private String lastQty;
    @JsonProperty("v") private String baseVolume;
    @JsonProperty("q") private String quoteVolume;
    @JsonProperty("n") private long   tradeCount;
    @JsonProperty("eventTimeKst") private String eventTimeKst;

    public long   getEventTime()          { return eventTime; }
    public String getSymbol()             { return symbol; }
    public String getLastPrice()          { return lastPrice; }
    public String getOpenPrice()          { return openPrice; }
    public String getHighPrice()          { return highPrice; }
    public String getLowPrice()           { return lowPrice; }
    public String getPrevClosePrice()     { return prevClosePrice; }
    public String getWeightedAvgPrice()   { return weightedAvgPrice; }
    public String getPriceChange()        { return priceChange; }
    public String getPriceChangePercent() { return priceChangePercent; }
    public String getBidPrice()           { return bidPrice; }
    public String getBidQty()             { return bidQty; }
    public String getAskPrice()           { return askPrice; }
    public String getAskQty()             { return askQty; }
    public String getLastQty()            { return lastQty; }
    public String getBaseVolume()         { return baseVolume; }
    public String getQuoteVolume()        { return quoteVolume; }
    public long   getTradeCount()         { return tradeCount; }
    public String getEventTimeKst()       { return eventTimeKst; }
}
