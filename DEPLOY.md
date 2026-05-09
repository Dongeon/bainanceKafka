# Oracle Cloud Free Tier 배포 가이드

## 전체 아키텍처

```
Internet
   │
   ▼
Oracle VM (ARM A1 · 4 OCPU · 24GB · Ubuntu 22.04)
   │
   ├── :80  → [nginx]           → 프론트엔드 React 정적 파일
   │
   └── :8081 → [bainanceapi]    → Spring Boot (REST API + WebSocket)
                     │
         ┌──────────┴──────────┐
         │                     │
    [MariaDB]              [Kafka]
    bainance DB            :9092
    isetdx_kafka DB
         ▲                     ▲
         │                     │
  [bainanceconsumer]   [isetdxKafkaProducer] → Binance WebSocket
  [isetdxBainanceConsumer]
```

---

## Step 1. 사전 작업 (로컬 Mac)

### 1-1. MariaDB 스키마 덤프 (init.sql 생성)

로컬 DB에서 테이블 구조를 덤프하여 Docker MariaDB 초기화 SQL을 만든다.

```bash
# 터미널에서 실행 (비밀번호 입력 프롬프트 나옴)
cd /Users/deadde/projects/java_bainance

# bainance DB 스키마 덤프 (데이터 제외, 구조만)
mysqldump -h 127.0.0.1 -P 33061 -u deadde -p \
  --no-data --routines --triggers \
  bainance > dump_bainance.sql

# isetdx_kafka DB 스키마 덤프
mysqldump -h 127.0.0.1 -P 33061 -u isetdx_kafka -p \
  --no-data --routines --triggers \
  isetdx_kafka > dump_isetdx.sql
```

덤프 파일 2개를 하나의 `init.sql`로 합친다.

```bash
cat > init.sql << 'INIT_SQL'
-- ── 사용자 & 데이터베이스 생성 ──────────────────────────────────
CREATE DATABASE IF NOT EXISTS bainance DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS isetdx_kafka DEFAULT CHARACTER SET utf8mb4;

CREATE USER IF NOT EXISTS 'deadde'@'%' IDENTIFIED BY 'qwer1234';
CREATE USER IF NOT EXISTS 'isetdx_kafka'@'%' IDENTIFIED BY 'isetdx1234';

GRANT ALL PRIVILEGES ON bainance.*     TO 'deadde'@'%';
GRANT ALL PRIVILEGES ON isetdx_kafka.* TO 'isetdx_kafka'@'%';
FLUSH PRIVILEGES;

INIT_SQL

# 덤프 내용 이어붙이기
echo "USE bainance;" >> init.sql
cat dump_bainance.sql >> init.sql

echo "USE isetdx_kafka;" >> init.sql
cat dump_isetdx.sql >> init.sql

# 덤프 파일 정리
rm dump_bainance.sql dump_isetdx.sql
```

---

### 1-2. 소스코드 설정 변경 (localhost → Docker 서비스명)

Docker 컨테이너 간 통신은 `localhost` 대신 `docker-compose.yml`의 서비스명을 사용한다.

**① `bainanceconsumer/src/main/java/org/example/Main.java`**

```java
// 변경 전
private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
private static final String DB_URL = "jdbc:mariadb://localhost:33061/bainance?serverTimezone=Asia/Seoul";

// 변경 후
private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
private static final String DB_URL = "jdbc:mariadb://mariadb:3306/bainance?serverTimezone=Asia/Seoul";
```

**② `isetdxKafkaProducer/kafka.conf`**

```properties
# 변경 전
kafka.bootstrap.servers=localhost:9092

# 변경 후
kafka.bootstrap.servers=kafka:9092
```

**③ `isetdxBainanceConsumer/kafka.conf`**

```properties
# 변경 전
kafka.bootstrap.servers=localhost:9092
db.url=jdbc:mariadb://localhost:33061/isetdx_kafka?serverTimezone=Asia/Seoul

# 변경 후
kafka.bootstrap.servers=kafka:9092
db.url=jdbc:mariadb://mariadb:3306/isetdx_kafka?serverTimezone=Asia/Seoul
```

**④ `bainanceapi/src/main/resources/application.yml`**

```yaml
# 변경 전
spring:
  kafka:
    bootstrap-servers: localhost:9092
  datasource:
    url: jdbc:mariadb://localhost:33061/bainance?serverTimezone=Asia/Seoul
isetdx:
  datasource:
    url: jdbc:mariadb://localhost:33061/isetdx_kafka?serverTimezone=Asia/Seoul

# 변경 후
spring:
  kafka:
    bootstrap-servers: kafka:9092
  datasource:
    url: jdbc:mariadb://mariadb:3306/bainance?serverTimezone=Asia/Seoul
isetdx:
  datasource:
    url: jdbc:mariadb://mariadb:3306/isetdx_kafka?serverTimezone=Asia/Seoul
```

---

### 1-3. Frontend URL 수정

> VM 생성 후 공인 IP를 알아낸 다음 이 파일을 수정한다.
> VM IP를 아직 모르면 이 단계는 Step 2 이후에 진행해도 된다.

**`frontend/crypto-dashboard/src/constants.ts`**

```typescript
// 변경 전
export const API_BASE = 'http://localhost:8081';
export const WS_URL   = 'ws://localhost:8081/ws';

// 변경 후 (<VM_PUBLIC_IP> 자리에 실제 IP 입력)
export const API_BASE = 'http://<VM_PUBLIC_IP>:8081';
export const WS_URL   = 'ws://<VM_PUBLIC_IP>:8081/ws';
```

---

### 1-4. Dockerfile 작성 (서비스별)

#### `bainanceconsumer/Dockerfile`

```dockerfile
FROM gradle:8.5-jdk17-alpine AS build
WORKDIR /app
COPY . .
RUN gradle installDist -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/build/install/bainanceconsumer/ .
ENTRYPOINT ["./bin/bainanceconsumer"]
```

#### `isetdxKafkaProducer/Dockerfile`

> 빌드 컨텍스트가 **repo 루트**여야 한다 (로컬 lib JAR 의존성 때문).

```dockerfile
FROM gradle:8.5-jdk17-alpine AS build
WORKDIR /workspace
# 로컬 라이브러리 먼저 빌드
COPY IsetDx_kafka-producer-lib/ ./IsetDx_kafka-producer-lib/
COPY IsetDx_kafka-leader-lib/   ./IsetDx_kafka-leader-lib/
RUN cd IsetDx_kafka-producer-lib && gradle build -x test --no-daemon
RUN cd IsetDx_kafka-leader-lib   && gradle build -x test --no-daemon
# 앱 빌드
COPY isetdxKafkaProducer/ ./isetdxKafkaProducer/
RUN cd isetdxKafkaProducer && gradle installDist -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/isetdxKafkaProducer/build/install/isetdxKafkaProducer/ .
COPY isetdxKafkaProducer/kafka.conf ./kafka.conf
ENV INSTANCE_ID=producer-1
ENV WEIGHT=100
CMD ["sh", "-c", "./bin/isetdxKafkaProducer kafka.conf ${INSTANCE_ID} ${WEIGHT}"]
```

#### `isetdxBainanceConsumer/Dockerfile`

> 빌드 컨텍스트가 **repo 루트**여야 한다.

```dockerfile
FROM gradle:8.5-jdk17-alpine AS build
WORKDIR /workspace
# 로컬 라이브러리 먼저 빌드
COPY IsetDx_kafka-consumer-lib/ ./IsetDx_kafka-consumer-lib/
COPY IsetDx_kafka-leader-lib/   ./IsetDx_kafka-leader-lib/
RUN cd IsetDx_kafka-consumer-lib && gradle build -x test --no-daemon
RUN cd IsetDx_kafka-leader-lib   && gradle build -x test --no-daemon
# 앱 빌드
COPY isetdxBainanceConsumer/ ./isetdxBainanceConsumer/
RUN cd isetdxBainanceConsumer && gradle installDist -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/isetdxBainanceConsumer/build/install/isetdxBainanceConsumer/ .
COPY isetdxBainanceConsumer/kafka.conf ./kafka.conf
ENTRYPOINT ["./bin/isetdxBainanceConsumer"]
```

#### `bainanceapi/Dockerfile`

```dockerfile
FROM gradle:8.5-jdk17-alpine AS build
WORKDIR /app
COPY . .
RUN gradle bootJar -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

#### `frontend/crypto-dashboard/Dockerfile`

```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

---

### 1-5. nginx.conf 작성

**`frontend/crypto-dashboard/nginx.conf`**

```nginx
server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    # React SPA 라우팅 (새로고침 시 404 방지)
    location / {
        try_files $uri $uri/ /index.html;
    }

    # 정적 파일 캐싱
    location ~* \.(js|css|png|jpg|gif|ico|svg|woff2?)$ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    gzip on;
    gzip_types text/plain text/css application/javascript application/json;
}
```

> API와 WebSocket은 `constants.ts`에서 VM 공인 IP로 직접 연결하므로 nginx 프록시 불필요.

---

### 1-6. docker-compose.yml 작성

프로젝트 루트에 생성한다.

```yaml
services:

  # ── 인프라 ─────────────────────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    restart: unless-stopped
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    restart: unless-stopped
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 24
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 15s
      timeout: 10s
      retries: 5

  mariadb:
    image: mariadb:11
    restart: unless-stopped
    environment:
      MARIADB_ROOT_PASSWORD: rootpass123
    volumes:
      - mariadb_data:/var/lib/mysql
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "healthcheck.sh", "--connect"]
      interval: 10s
      timeout: 5s
      retries: 10

  # ── 애플리케이션 ────────────────────────────────────────────────
  isetdxKafkaProducer:
    build:
      context: .
      dockerfile: isetdxKafkaProducer/Dockerfile
    restart: unless-stopped
    depends_on:
      kafka:
        condition: service_healthy

  bainanceconsumer:
    build:
      context: ./bainanceconsumer
    restart: unless-stopped
    depends_on:
      kafka:
        condition: service_healthy
      mariadb:
        condition: service_healthy

  isetdxBainanceConsumer:
    build:
      context: .
      dockerfile: isetdxBainanceConsumer/Dockerfile
    restart: unless-stopped
    depends_on:
      kafka:
        condition: service_healthy
      mariadb:
        condition: service_healthy

  bainanceapi:
    build:
      context: ./bainanceapi
    restart: unless-stopped
    ports:
      - "8081:8081"
    depends_on:
      kafka:
        condition: service_healthy
      mariadb:
        condition: service_healthy

  frontend:
    build:
      context: ./frontend/crypto-dashboard
    restart: unless-stopped
    ports:
      - "80:80"
    depends_on:
      - bainanceapi

volumes:
  mariadb_data:
```

---

### 1-7. git push

```bash
cd /Users/deadde/projects/java_bainance

git add \
  init.sql \
  docker-compose.yml \
  bainanceconsumer/Dockerfile \
  isetdxKafkaProducer/Dockerfile \
  isetdxBainanceConsumer/Dockerfile \
  bainanceapi/Dockerfile \
  frontend/crypto-dashboard/Dockerfile \
  frontend/crypto-dashboard/nginx.conf \
  bainanceconsumer/src/main/java/org/example/Main.java \
  isetdxKafkaProducer/kafka.conf \
  isetdxBainanceConsumer/kafka.conf \
  bainanceapi/src/main/resources/application.yml \
  frontend/crypto-dashboard/src/constants.ts

git commit -m "feat: add Docker Compose + Dockerfiles for Oracle Cloud deployment"
git push origin main
```

---

## Step 2. Oracle Cloud VM 생성

### 2-1. 계정 생성 & Free Tier 활성화

1. [cloud.oracle.com](https://cloud.oracle.com) 접속 → **Start for free**
2. 이메일, 이름, 국가(한국) 입력
3. 신용카드 등록 (Free Tier는 청구 안 됨, 본인 확인용)
4. 가입 완료 후 로그인

### 2-2. VM Instance 생성

좌측 메뉴 → **Compute** → **Instances** → **Create Instance**

| 설정 항목 | 값 |
|---|---|
| Name | bainance-server |
| Image | Canonical Ubuntu 22.04 |
| Shape | **VM.Standard.A1.Flex** |
| OCPU | **4** |
| Memory (GB) | **24** |
| Network | 기본 VCN 사용 |
| Public IP | **Assign a public IPv4 address** 선택 |

### 2-3. SSH Key 등록

```bash
# 로컬 Mac에서 SSH Key가 없으면 생성
ssh-keygen -t ed25519 -C "oracle-bainance" -f ~/.ssh/oracle_bainance

# 공개키 내용 복사
cat ~/.ssh/oracle_bainance.pub
```

복사한 공개키를 Oracle 콘솔 **SSH keys** 항목에 붙여넣기 → **Create**

### 2-4. 공인 IP 확인

Instance 생성 완료 후 상세 페이지에서 **Public IP address** 값을 메모한다.

```
예시: 152.70.xxx.xxx
```

> 이 IP를 `frontend/crypto-dashboard/src/constants.ts`의 `<VM_PUBLIC_IP>` 자리에 입력하고, Step 1-3과 1-7을 진행한다.

---

## Step 3. 네트워크 보안 설정

Oracle Cloud는 **2단계** 방화벽이 있다. 둘 다 설정해야 한다.

### 3-1. Oracle Security List (콘솔에서)

**Networking → Virtual Cloud Networks → [VCN명] → Security Lists → Default Security List → Add Ingress Rules**

| Source CIDR | Protocol | Port | 용도 |
|---|---|---|---|
| 0.0.0.0/0 | TCP | 22 | SSH |
| 0.0.0.0/0 | TCP | 80 | 프론트엔드 |
| 0.0.0.0/0 | TCP | 8081 | API + WebSocket |

각각 **Add Ingress Rule** 버튼으로 추가한다.

### 3-2. VM 내부 OS 방화벽 (SSH 접속 후)

Ubuntu 22.04는 기본적으로 iptables가 외부 접근을 차단한다.

```bash
# SSH 접속 후 실행
sudo iptables -I INPUT -p tcp --dport 80   -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 8081 -j ACCEPT

# 재부팅 후에도 유지되도록 저장
sudo apt-get install -y iptables-persistent
sudo netfilter-persistent save
```

---

## Step 4. 서버 초기 세팅

### 4-1. SSH 접속

```bash
ssh -i ~/.ssh/oracle_bainance ubuntu@<VM_PUBLIC_IP>
```

### 4-2. 패키지 업데이트

```bash
sudo apt-get update && sudo apt-get upgrade -y
```

### 4-3. Docker 설치

```bash
# Docker 공식 설치 스크립트
curl -fsSL https://get.docker.com | sh

# 현재 사용자를 docker 그룹에 추가 (sudo 없이 docker 사용)
sudo usermod -aG docker ubuntu

# 그룹 적용 (재로그인 없이 즉시 적용)
newgrp docker

# Docker Compose plugin 설치
sudo apt-get install -y docker-compose-plugin

# 설치 확인
docker --version
docker compose version
```

### 4-4. Swap 설정 (권장)

Gradle 빌드 시 메모리가 부족할 수 있으니 swap을 추가한다.

```bash
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# 재부팅 후에도 유지
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# 확인
free -h
```

---

## Step 5. 배포 실행

### 5-1. 소스코드 clone

```bash
cd ~
git clone https://github.com/Dongeon/bainanceKafka.git
cd bainanceKafka
```

### 5-2. 빌드 & 실행

> 최초 빌드는 Gradle 의존성 다운로드 + Docker 이미지 빌드로 **15~25분** 소요된다.

```bash
docker compose up --build -d
```

실행 중 로그 실시간 확인:

```bash
docker compose logs -f
```

특정 서비스만 보려면:

```bash
docker compose logs -f kafka
docker compose logs -f bainanceapi
docker compose logs -f isetdxKafkaProducer
```

---

## Step 6. 확인

### 서비스 상태 확인

```bash
docker compose ps
```

모든 서비스가 `Up` 상태여야 한다.

```
NAME                      STATUS
bainanceKafka-zookeeper-1          Up
bainanceKafka-kafka-1              Up (healthy)
bainanceKafka-mariadb-1            Up (healthy)
bainanceKafka-isetdxKafkaProducer-1  Up
bainanceKafka-bainanceconsumer-1   Up
bainanceKafka-isetdxBainanceConsumer-1  Up
bainanceKafka-bainanceapi-1        Up
bainanceKafka-frontend-1           Up
```

### 접속 확인

| 주소 | 내용 |
|---|---|
| `http://<VM_IP>` | 프론트엔드 대시보드 |
| `http://<VM_IP>:8081/api/tickers/latest` | API 응답 확인 |
| `http://<VM_IP>:8081/api/producers/status` | Producer 상태 확인 |

---

## Step 7. 운영 명령어

### 전체 중지

```bash
docker compose down
```

### 전체 재시작

```bash
docker compose restart
```

### 소스 업데이트 후 재배포

```bash
git pull origin main
docker compose up --build -d
```

### 특정 서비스만 재빌드

```bash
docker compose up --build -d bainanceapi
```

### 로그 확인 (마지막 100줄)

```bash
docker compose logs --tail=100 bainanceapi
```

### MariaDB 직접 접속

```bash
docker compose exec mariadb mariadb -u deadde -pqwer1234 bainance
```

### 디스크 / 메모리 확인

```bash
df -h        # 디스크
free -h      # 메모리
docker stats # 컨테이너별 리소스 사용량
```

---

## 트러블슈팅

### Kafka 연결 실패 (`Connection refused kafka:9092`)

Kafka가 아직 준비 안 된 것. `healthcheck` 통과 후 앱이 뜨도록 설계되어 있지만, 타이밍 문제가 생기면:

```bash
docker compose restart bainanceconsumer isetdxBainanceConsumer bainanceapi
```

### MariaDB 초기화 실패

`init.sql` 문법 오류. 로그 확인 후 수정:

```bash
docker compose logs mariadb
# 수정 후
docker compose down -v   # 볼륨까지 삭제 (초기화 재실행)
docker compose up -d mariadb
```

> ⚠️ `-v` 옵션은 DB 데이터를 모두 삭제하므로 주의.

### Gradle 빌드 메모리 부족 (`OutOfMemoryError`)

```bash
# swap이 설정되어 있는지 확인
free -h

# 없으면 Step 4-4 다시 실행
```

### 포트 80/8081 접속 불가

```bash
# iptables 규칙 확인
sudo iptables -L INPUT -n

# 없으면 추가
sudo iptables -I INPUT -p tcp --dport 80   -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 8081 -j ACCEPT
sudo netfilter-persistent save

# Oracle Security List도 확인 (콘솔에서)
```

### 프론트엔드에서 API 연결 안 됨

`constants.ts`의 IP가 맞는지 확인. 수정 후 재빌드:

```bash
# 로컬에서
# constants.ts 수정 → git push

# VM에서
git pull
docker compose up --build -d frontend
```
