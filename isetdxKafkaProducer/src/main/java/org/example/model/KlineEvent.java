package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 바이낸스 kline(캔들스틱) 스트림 메시지 모델.
 *
 * <p>바이낸스 kline 메시지는 중첩 구조다. "k" 필드 안에 캔들 데이터가 담겨 있다.
 * 이 클래스는 역직렬화(Binance → Java)와 직렬화(Java → Kafka JSON) 방향을 분리해서 처리한다.
 *
 * <p>역직렬화: WRITE_ONLY 필드로 바이낸스 "s", "k" 필드를 읽는다.
 * <p>직렬화: 플랫한 getter 메서드로 Kafka JSON을 생성한다.
 *
 * <p>Kafka에 전송되는 JSON 예시:
 * <pre>
 * {
 *   "symbol": "BTCUSDT", "interval": "1m",
 *   "openTimeKst": "2024-01-01 09:00:00.000",
 *   "closeTimeKst": "2024-01-01 09:00:59.999",
 *   "openPrice": "50000.00", "highPrice": "50200.00",
 *   "lowPrice": "49900.00",  "closePrice": "50100.00",
 *   "volume": "100.5",       "quoteVolume": "5025000.00",
 *   "tradeCount": 500
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KlineEvent {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter KST_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // ── Binance 역직렬화 전용 (WRITE_ONLY → Kafka JSON에 포함되지 않음) ────────
    // 필드명을 rawXxx로 지어야 Jackson이 getter와 연결하지 않는다.
    // getSymbol() ↔ symbol 필드가 같은 이름이면 WRITE_ONLY가 getter 직렬화까지 막는다.
    @JsonProperty(value = "s", access = JsonProperty.Access.WRITE_ONLY)
    private String rawSymbol;

    @JsonProperty(value = "k", access = JsonProperty.Access.WRITE_ONLY)
    private KlineData kline;

    // ── Kafka 직렬화 (플랫 JSON getter) ─────────────────────────────────────

    @JsonProperty("symbol")
    public String getSymbol() { return rawSymbol; }

    @JsonProperty("interval")
    public String getInterval() { return kline.interval; }

    @JsonProperty("openTimeKst")
    public String getOpenTimeKst() {
        return Instant.ofEpochMilli(kline.openTime).atZone(KST).format(KST_FMT);
    }

    @JsonProperty("closeTimeKst")
    public String getCloseTimeKst() {
        return Instant.ofEpochMilli(kline.closeTime).atZone(KST).format(KST_FMT);
    }

    @JsonProperty("openPrice")
    public String getOpenPrice()  { return kline.openPrice; }

    @JsonProperty("highPrice")
    public String getHighPrice()  { return kline.highPrice; }

    @JsonProperty("lowPrice")
    public String getLowPrice()   { return kline.lowPrice; }

    @JsonProperty("closePrice")
    public String getClosePrice() { return kline.closePrice; }

    @JsonProperty("volume")
    public String getVolume()     { return kline.volume; }

    @JsonProperty("quoteVolume")
    public String getQuoteVolume() { return kline.quoteVolume; }

    @JsonProperty("tradeCount")
    public int getTradeCount()    { return kline.tradeCount; }

    /** 캔들 확정 여부. true일 때만 Kafka로 전송한다. Kafka JSON에는 포함하지 않는다. */
    @JsonIgnore
    public boolean isClosed() { return kline != null && kline.closed; }

    @Override
    public String toString() {
        if (kline == null) return "[" + rawSymbol + "] (no kline data)";
        return String.format("[%s@%s] o=%s h=%s l=%s c=%s vol=%s closed=%b",
                rawSymbol, kline.interval, kline.openPrice, kline.highPrice,
                kline.lowPrice, kline.closePrice, kline.volume, kline.closed);
    }

    // ── 바이낸스 "k" 필드 내부 구조 ──────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KlineData {
        @JsonProperty("i") String  interval;
        @JsonProperty("t") long    openTime;   // 캔들 시작 UTC ms
        @JsonProperty("T") long    closeTime;  // 캔들 종료 UTC ms
        @JsonProperty("o") String  openPrice;
        @JsonProperty("c") String  closePrice;
        @JsonProperty("h") String  highPrice;
        @JsonProperty("l") String  lowPrice;
        @JsonProperty("v") String  volume;
        @JsonProperty("q") String  quoteVolume;
        @JsonProperty("n") int     tradeCount;
        @JsonProperty("x") boolean closed;     // true = 캔들 확정
    }
}
