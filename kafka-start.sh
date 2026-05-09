#!/usr/bin/env bash
# ============================================================
# Kafka + Zookeeper 기동 스크립트
# ============================================================

KAFKA_HOME="/Users/deadde/projects/kafka_bainance"
LOG_DIR="/tmp/kafka-start-logs"

mkdir -p "$LOG_DIR"

# ── 이미 실행 중인지 확인 ──────────────────────────────────────
is_running() {
    pgrep -f "$1" > /dev/null 2>&1
}

echo "=============================="
echo "  Kafka 기동 스크립트"
echo "=============================="

# ── 1. Zookeeper 시작 ─────────────────────────────────────────
if is_running "zookeeper"; then
    echo "[SKIP] Zookeeper 이미 실행 중"
else
    echo "[START] Zookeeper 기동 중..."
    "$KAFKA_HOME/bin/zookeeper-server-start.sh" \
        "$KAFKA_HOME/config/zookeeper.properties" \
        > "$LOG_DIR/zookeeper.log" 2>&1 &
    ZK_PID=$!
    echo "        PID: $ZK_PID"

    # Zookeeper 준비 대기 (최대 15초)
    echo -n "        대기 중"
    for i in $(seq 1 15); do
        sleep 1
        echo -n "."
        if nc -z localhost 2181 > /dev/null 2>&1; then
            echo " 준비 완료"
            break
        fi
        if [ $i -eq 15 ]; then
            echo " 타임아웃 — 로그 확인: $LOG_DIR/zookeeper.log"
            exit 1
        fi
    done
fi

# ── 2. Kafka Broker 시작 ──────────────────────────────────────
if is_running "kafka.Kafka"; then
    echo "[SKIP] Kafka Broker 이미 실행 중"
else
    echo "[START] Kafka Broker 기동 중..."
    "$KAFKA_HOME/bin/kafka-server-start.sh" \
        "$KAFKA_HOME/config/server.properties" \
        > "$LOG_DIR/kafka.log" 2>&1 &
    KAFKA_PID=$!
    echo "        PID: $KAFKA_PID"

    # Kafka 준비 대기 (최대 20초)
    echo -n "        대기 중"
    for i in $(seq 1 20); do
        sleep 1
        echo -n "."
        if nc -z localhost 9092 > /dev/null 2>&1; then
            echo " 준비 완료"
            break
        fi
        if [ $i -eq 20 ]; then
            echo " 타임아웃 — 로그 확인: $LOG_DIR/kafka.log"
            exit 1
        fi
    done
fi

# ── 완료 ──────────────────────────────────────────────────────
echo ""
echo "=============================="
echo "  기동 완료"
echo "  Zookeeper : localhost:2181"
echo "  Kafka     : localhost:9092"
echo "  로그 위치  : $LOG_DIR/"
echo "=============================="
