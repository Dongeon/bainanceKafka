package org.example.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Binance REST API에서 심볼 목록을 가져오는 유틸리티 클래스.
 *
 * <p>fetch 방식 두 가지:
 *   - fetchUsdtSymbols(limit)   : TRADING 상태 USDT 마켓만 (최대 ~660개)
 *   - fetchAllSymbols(limit)    : TRADING 상태 전체 마켓 (최대 ~1403개, USDT 우선 정렬)
 *
 * <p>WebSocket 연결당 스트림 한계:
 *   - Ticker:  최대 1024 심볼
 *   - Kline:   최대 1024 / 인터벌 수 심볼
 *   한계 초과 여부 관리는 호출자(Main)가 담당한다.
 */
public class BinanceSymbolFetcher {

    private static final Logger log = LoggerFactory.getLogger(BinanceSymbolFetcher.class);
    private static final String EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo";

    private BinanceSymbolFetcher() {}

    /**
     * TRADING 상태인 USDT 마켓 심볼을 최대 {@code limit}개 반환한다. (Phase 1~3용)
     */
    public static List<String> fetchUsdtSymbols(int limit) throws IOException {
        return fetch(limit, "USDT");
    }

    /**
     * TRADING 상태인 전체 마켓 심볼을 최대 {@code limit}개 반환한다. (Phase 4용)
     * USDT 쌍을 먼저 채우고, 부족하면 BTC → BNB → 나머지 순으로 채운다.
     */
    public static List<String> fetchAllSymbols(int limit) throws IOException {
        OkHttpClient client = buildClient();
        Request request = new Request.Builder().url(EXCHANGE_INFO_URL).build();

        log.info("Binance exchangeInfo 요청 중 (전체 마켓)...");
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new IOException("Binance API 오류: HTTP " + response.code());

            ObjectMapper mapper = new ObjectMapper();
            JsonNode symbolNodes = mapper.readTree(response.body().string()).get("symbols");

            List<String> usdt = new ArrayList<>();
            List<String> btc  = new ArrayList<>();
            List<String> bnb  = new ArrayList<>();
            List<String> rest = new ArrayList<>();

            for (JsonNode node : symbolNodes) {
                if (!"TRADING".equals(node.get("status").asText())) continue;
                String sym   = node.get("symbol").asText();
                String quote = node.get("quoteAsset").asText();
                switch (quote) {
                    case "USDT" -> usdt.add(sym);
                    case "BTC"  -> btc.add(sym);
                    case "BNB"  -> bnb.add(sym);
                    default     -> rest.add(sym);
                }
            }

            List<String> result = new ArrayList<>(limit);
            for (List<String> bucket : List.of(usdt, btc, bnb, rest)) {
                for (String sym : bucket) {
                    result.add(sym);
                    if (result.size() >= limit) break;
                }
                if (result.size() >= limit) break;
            }

            log.info("심볼 {}개 로드 완료 (요청: {}, 첫 번째: {}, 마지막: {})",
                    result.size(), limit, result.get(0), result.get(result.size() - 1));
            return result;
        } finally {
            client.dispatcher().executorService().shutdown();
        }
    }

    private static List<String> fetch(int limit, String quoteAsset) throws IOException {
        OkHttpClient client = buildClient();
        Request request = new Request.Builder().url(EXCHANGE_INFO_URL).build();

        log.info("Binance exchangeInfo 요청 중 (quoteAsset={})...", quoteAsset);
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new IOException("Binance API 오류: HTTP " + response.code());

            ObjectMapper mapper = new ObjectMapper();
            JsonNode symbolNodes = mapper.readTree(response.body().string()).get("symbols");

            List<String> result = new ArrayList<>();
            for (JsonNode node : symbolNodes) {
                if (!"TRADING".equals(node.get("status").asText())) continue;
                if (!quoteAsset.equals(node.get("quoteAsset").asText())) continue;
                result.add(node.get("symbol").asText());
                if (result.size() >= limit) break;
            }

            log.info("심볼 {}개 로드 완료 (요청: {}, 첫 번째: {}, 마지막: {})",
                    result.size(), limit, result.get(0), result.get(result.size() - 1));
            return result;
        } finally {
            client.dispatcher().executorService().shutdown();
        }
    }

    private static OkHttpClient buildClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }
}
