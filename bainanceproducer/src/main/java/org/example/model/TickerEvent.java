package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 바이낸스 24hrTicker 스트림 메시지를 담는 모델 클래스.
 *
 * <p>바이낸스 JSON 필드명은 단일 소문자/대문자 약어(예: "c", "E")를 사용한다.
 * Jackson의 @JsonProperty로 이 약어를 의미 있는 Java 필드명에 매핑한다.
 *
 * <p>eventTimeKst 필드는 바이낸스 원본에 없는 계산 필드다.
 * UTC milliseconds(eventTime)를 KST 문자열로 변환하여 Kafka 전송 JSON에 추가한다.
 * Consumer/DB 쪽에서 변환 로직 없이 바로 저장할 수 있도록 Producer에서 미리 변환한다.
 *
 * <p>@JsonIgnoreProperties(ignoreUnknown = true):
 * 바이낸스가 새 필드를 추가하더라도 파싱 오류 없이 무시하도록 설정.
 *
 * <p>바이낸스 공식 문서:
 * https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-ticker-streams
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TickerEvent {

    /** KST(한국 표준시) ZoneId. eventTime 변환에 사용. */
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /** KST 출력 포맷. MariaDB의 DATETIME(3) 컬럼과 포맷을 맞춘다. */
    private static final DateTimeFormatter KST_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // ── 식별 ────────────────────────────────────────────────────────────────
    @JsonProperty("E") private long   eventTime;           // 이벤트 발생 시각 UTC (ms)
    @JsonProperty("s") private String symbol;              // 심볼 (예: BTCUSDT)

    // ── 가격 ────────────────────────────────────────────────────────────────
    @JsonProperty("c") private String lastPrice;           // 현재가 (최근 체결가)
    @JsonProperty("o") private String openPrice;           // 24시간 전 시가
    @JsonProperty("h") private String highPrice;           // 24시간 기준 최고가
    @JsonProperty("l") private String lowPrice;            // 24시간 기준 최저가
    @JsonProperty("x") private String prevClosePrice;      // 전일 종가
    @JsonProperty("w") private String weightedAvgPrice;    // 24시간 가중평균가 (= quoteVolume / baseVolume)

    // ── 변동 ────────────────────────────────────────────────────────────────
    @JsonProperty("p") private String priceChange;         // 24시간 가격 변동 (절댓값, USDT)
    @JsonProperty("P") private String priceChangePercent;  // 24시간 변동률 (%)

    // ── 호가 ────────────────────────────────────────────────────────────────
    @JsonProperty("b") private String bidPrice;            // 최우선 매수 호가 (가장 높은 매수 대기가)
    @JsonProperty("B") private String bidQty;              // 최우선 매수 수량
    @JsonProperty("a") private String askPrice;            // 최우선 매도 호가 (가장 낮은 매도 대기가)
    @JsonProperty("A") private String askQty;              // 최우선 매도 수량
    @JsonProperty("Q") private String lastQty;             // 마지막 체결 수량

    // ── 거래량 ──────────────────────────────────────────────────────────────
    @JsonProperty("v") private String baseVolume;          // 코인 기준 거래량 (예: BTC 단위)
    @JsonProperty("q") private String quoteVolume;         // USDT 기준 거래량 (거래대금)
    @JsonProperty("n") private long   tradeCount;          // 24시간 총 체결 횟수

    // ── Getters ─────────────────────────────────────────────────────────────
    public long   getEventTime()          { return eventTime; }

    /**
     * eventTime(UTC ms)을 KST 문자열로 변환하여 반환한다.
     *
     * @JsonProperty("eventTimeKst")로 인해 Jackson이 JSON 직렬화 시
     * "eventTimeKst" 키로 이 값을 자동으로 포함시킨다.
     * MariaDB의 event_time 컬럼(KST DATETIME)에 바로 저장 가능한 포맷이다.
     *
     * 예: 1714999200000L → "2024-05-07 00:00:00.000"
     */
    @JsonProperty("eventTimeKst")
    public String getEventTimeKst() {
        return Instant.ofEpochMilli(eventTime).atZone(KST).format(KST_FMT);
    }

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

    /** 로그 출력용 요약 문자열. 전체 필드 대신 핵심 값만 보여준다. */
    @Override
    public String toString() {
        return String.format("[%s] price=%s change=%s%% vol=%s trades=%d",
                symbol, lastPrice, priceChangePercent, baseVolume, tradeCount);
    }
}
