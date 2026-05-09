package org.example.kafka.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka AdminClient로 클러스터 내 모든 토픽을 자동 감지해
 * 생산량(offset delta)과 consumer group lag을 주기적으로 DB에 적재한다.
 *
 * <p>토픽 목록은 하드코딩 없이 AdminClient.listTopics()로 동적 수집한다.
 *
 * <p>사용 예:
 * <pre>{@code
 *   TopicMonitor monitor = TopicMonitor.fromConfig("kafka.conf").build();
 *   monitor.start();
 *   // ...
 *   monitor.stop();
 * }</pre>
 */
public class TopicMonitor {

    private static final Logger log = LoggerFactory.getLogger(TopicMonitor.class);

    // ── Builder ──────────────────────────────────────────────────────────────

    public static Builder fromConfig(String configPath) throws IOException {
        return new Builder(ConsumerSettings.load(configPath));
    }

    public static Builder fromSettings(ConsumerSettings settings) {
        return new Builder(settings);
    }

    public static class Builder {
        private final ConsumerSettings settings;

        private Builder(ConsumerSettings settings) {
            this.settings = settings;
        }

        public TopicMonitor build() {
            return new TopicMonitor(this);
        }
    }

    // ── 필드 ─────────────────────────────────────────────────────────────────

    private final ConsumerSettings       settings;
    private final TopicMonitorRepository repository;

    // 토픽별 이전 스냅샷 offset 합계 (delta 계산용)
    private final Map<String, Long> previousOffsets = new ConcurrentHashMap<>();

    private final AtomicBoolean      running   = new AtomicBoolean(false);
    private AdminClient              adminClient;
    private ScheduledExecutorService scheduler;

    private TopicMonitor(Builder builder) {
        this.settings   = builder.settings;
        this.repository = new TopicMonitorRepository(builder.settings);
    }

    // ── 공개 API ─────────────────────────────────────────────────────────────

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("TopicMonitor already running");
            return;
        }

        adminClient = AdminClient.create(settings.toAdminClientProperties());

        final TopicMonitor self = this;
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "topic-monitor");
                t.setDaemon(true);
                return t;
            }
        });

        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    self.collectProductionStats();
                    self.collectConsumerGroupLag();
                } catch (Exception e) {
                    log.error("TopicMonitor 수집 실패: {}", e.getMessage());
                }
            }
        }, 0, settings.topicMonitorIntervalMs(), TimeUnit.MILLISECONDS);

        log.info("TopicMonitor 시작 (interval={}ms, excludePrefix='{}')",
                settings.topicMonitorIntervalMs(), settings.topicExcludePrefix());
    }

    public void stop() {
        running.set(false);
        if (scheduler   != null) scheduler.shutdownNow();
        if (adminClient != null) adminClient.close();
        repository.close();
        log.info("TopicMonitor 종료");
    }

    // ── 토픽 생산량 수집 ─────────────────────────────────────────────────────

    private void collectProductionStats() throws Exception {
        String excludePrefix = settings.topicExcludePrefix();

        // 1. 전체 토픽 목록 조회 및 내부 토픽 제외
        Set<String> allTopics = adminClient.listTopics().names().get();
        Set<String> targets = new HashSet<>();
        for (String t : allTopics) {
            if (!t.startsWith(excludePrefix)) {
                targets.add(t);
            }
        }
        if (targets.isEmpty()) return;

        // 2. 파티션 정보 조회
        Map<String, TopicDescription> descriptions =
                adminClient.describeTopics(targets).allTopicNames().get();

        // 3. 각 파티션의 최신 offset 조회 요청 맵 구성
        Map<TopicPartition, OffsetSpec> offsetQuery = new HashMap<>();
        Map<String, Integer> partitionCounts = new HashMap<>();

        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            String topicName = entry.getKey();
            List<TopicPartitionInfo> partitions = entry.getValue().partitions();
            partitionCounts.put(topicName, partitions.size());
            for (TopicPartitionInfo p : partitions) {
                offsetQuery.put(new TopicPartition(topicName, p.partition()), OffsetSpec.latest());
            }
        }

        // 4. 최신 offset 조회
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                adminClient.listOffsets(offsetQuery).all().get();

        // 5. 토픽별 offset 합산
        Map<String, Long> currentTotals = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
            String topicName = entry.getKey().topic();
            long   offset    = entry.getValue().offset();
            Long   existing  = currentTotals.get(topicName);
            currentTotals.put(topicName, existing == null ? offset : existing + offset);
        }

        // 6. delta 계산 후 DB 적재
        for (Map.Entry<String, Long> entry : currentTotals.entrySet()) {
            String topicName = entry.getKey();
            long   current   = entry.getValue();
            Long   previous  = previousOffsets.get(topicName);
            long   delta     = (previous == null) ? 0 : Math.max(0, current - previous);
            previousOffsets.put(topicName, current);

            int partitionCount = partitionCounts.containsKey(topicName)
                    ? partitionCounts.get(topicName) : 0;
            repository.insertProductionStats(topicName, partitionCount, current, delta);
        }

        log.debug("[TopicMonitor] 생산량 수집 완료: {}개 토픽", currentTotals.size());
    }

    // ── Consumer Group Lag 수집 ──────────────────────────────────────────────

    private void collectConsumerGroupLag() throws Exception {
        Collection<ConsumerGroupListing> groups =
                adminClient.listConsumerGroups().all().get();

        for (ConsumerGroupListing group : groups) {
            String groupId = group.groupId();

            // 1. 그룹의 커밋된 offset 조회
            Map<TopicPartition, OffsetAndMetadata> committed =
                    adminClient.listConsumerGroupOffsets(groupId)
                               .partitionsToOffsetAndMetadata().get();

            if (committed.isEmpty()) continue;

            // 2. 해당 파티션의 최신 offset 조회
            Map<TopicPartition, OffsetSpec> latestQuery = new HashMap<>();
            for (TopicPartition tp : committed.keySet()) {
                latestQuery.put(tp, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    adminClient.listOffsets(latestQuery).all().get();

            // 3. 토픽별 lag 합산
            Map<String, Long> lagPerTopic = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                TopicPartition tp              = entry.getKey();
                long           committedOffset = entry.getValue().offset();

                ListOffsetsResult.ListOffsetsResultInfo latestInfo = latestOffsets.get(tp);
                if (latestInfo == null) continue;

                long   lag       = Math.max(0, latestInfo.offset() - committedOffset);
                String topicName = tp.topic();
                Long   existing  = lagPerTopic.get(topicName);
                lagPerTopic.put(topicName, existing == null ? lag : existing + lag);
            }

            // 4. DB 적재
            for (Map.Entry<String, Long> lagEntry : lagPerTopic.entrySet()) {
                repository.insertConsumerGroupLag(groupId, lagEntry.getKey(), lagEntry.getValue());
            }
        }

        log.debug("[TopicMonitor] lag 수집 완료: {}개 그룹", groups.size());
    }
}
