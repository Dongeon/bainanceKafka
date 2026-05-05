package org.example.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON 페이로드에서 Kafka 파티션 키를 추출한다.
 *
 * <p>조건 6 (A+C 혼합) 구현:
 * <pre>
 *   A 방식: kafka.partition.key.field 가 설정된 경우
 *           → JSON에서 해당 필드 값을 추출해 키로 사용
 *           → 같은 필드 값은 항상 같은 파티션으로 라우팅 (시계열 순서 보장)
 *
 *   C 방식: 필드 미설정 / 필드 없음 / 필드 null 인 경우
 *           → null 반환 → Kafka가 라운드로빈으로 파티션 분배 (최대 처리량)
 * </pre>
 *
 * <p>이 클래스는 불변(stateless)이며 멀티스레드 환경에서 안전하게 공유 가능하다.
 */
public class PartitionKeyExtractor {

    private static final Logger log = LoggerFactory.getLogger(PartitionKeyExtractor.class);

    private final String keyField;
    private final ObjectMapper mapper;

    /**
     * @param keyField kafka.partition.key.field 값. 빈 문자열이면 라운드로빈 모드.
     */
    public PartitionKeyExtractor(String keyField) {
        this.keyField = (keyField == null) ? "" : keyField.trim();
        this.mapper   = new ObjectMapper();
    }

    /**
     * JSON 문자열에서 파티션 키를 추출한다.
     *
     * @param json 직렬화된 JSON 페이로드
     * @return 파티션 키 문자열, 또는 null (라운드로빈)
     */
    public String extract(String json) {
        if (keyField.isBlank()) return null;  // C 방식: 라운드로빈

        try {
            JsonNode node  = mapper.readTree(json);
            JsonNode field = node.get(keyField);

            if (field == null || field.isNull()) {
                log.debug("Partition key field '{}' not found in payload → round-robin", keyField);
                return null;  // 필드 없으면 C 방식으로 폴백
            }
            return field.asText();  // A 방식: 필드 값을 키로

        } catch (Exception e) {
            log.warn("Failed to extract partition key '{}': {} → round-robin", keyField, e.getMessage());
            return null;  // 파싱 실패 시 C 방식으로 폴백
        }
    }
}
