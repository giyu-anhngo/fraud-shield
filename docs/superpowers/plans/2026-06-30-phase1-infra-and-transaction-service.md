# Phase 1 Infrastructure & Transaction Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up `docker-compose.yml` (PostgreSQL + Kafka KRaft with volumes & healthchecks) and a runnable Transaction Service that records a payment to PostgreSQL and publishes a `TransactionEvent` to the `transactions` topic via `POST /transactions`.

**Architecture:** Event-driven microservices around a Kafka log. This phase delivers the infra plus the first service. The service is layered (web → service → repository/messaging). It saves the payment to PostgreSQL, then publishes the event directly to Kafka (the outbox pattern that closes the resulting dual-write gap is deferred to Phase 2). See `docs/superpowers/specs/2026-06-30-phase1-infra-and-transaction-service-design.md`.

**Tech Stack:** Java 21, Spring Boot 3.x (Web, Data JPA, Validation, Actuator, Spring Kafka), Flyway, PostgreSQL 16, Apache Kafka 3.9 (KRaft), Maven, Lombok, JUnit 5 + Mockito + spring-kafka-test.

## Global Constraints

- **Java 21.** The JDK is installed at `<path-to-your-jdk-21>`.
- **NEVER change the global/system `JAVA_HOME`** (it is your Java 8 install and is used by the user's main project). Set `$env:JAVA_HOME` **process-scoped only** at the top of each PowerShell command that runs Maven. See the "Build environment" preamble below.
- **Shell is Windows PowerShell 5.1.** No `&&`/`||` chaining; use `;` and `if ($?) { ... }`. Native git is not on PATH — use the GitHub Desktop git: `& "git"`. A `$git` variable is defined in the preamble below.
- **Docker is NOT installed** on this machine. Any `docker compose` step is a **manual step for the user** (clearly marked), not something the agentic worker runs.
- **Maven** is on PATH at `your Maven install` (`mvn`, v3.9.14).
- **Package root:** `com.fraudshield.transaction`. **Service port:** `8081`. **Topic:** `transactions`, keyed by `accountId`.
- **Event schema** must match `docs/architecture.md §4` exactly: `eventId, transactionId, accountId, amount, currency, country, merchantId, merchantCategory, channel, occurredAt`.
- **Flyway owns the schema**; JPA `ddl-auto: validate`.
- Outbox, idempotency, MongoDB, Zipkin, other services, Testcontainers, the API Gateway are **out of scope** for this phase.

### Build environment preamble (paste at the top of every Maven/git PowerShell command)

```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"   # process-scoped; does NOT touch global JAVA_HOME
```

> The working directory for all commands is the repo root `<repo-root>` unless a step says otherwise. Do not prefix commands with `cd` to the repo root — the harness already starts there.

---

## File Structure

```
fraud-shield/
├── docker-compose.yml                         # Task 1
├── infra/postgres/init/01-create-databases.sql # Task 1
├── docs/adr/0001-kafka-kraft-single-node.md   # Task 8
├── docs/adr/0002-direct-publish-before-outbox.md # Task 8
└── transaction-service/                        # Tasks 2-7
    ├── pom.xml                                  # Task 2
    ├── src/main/java/com/fraudshield/transaction/
    │   ├── TransactionServiceApplication.java   # Task 2
    │   ├── domain/Account.java                  # Task 3
    │   ├── domain/Transaction.java              # Task 3
    │   ├── repository/AccountRepository.java    # Task 3
    │   ├── repository/TransactionRepository.java# Task 3
    │   ├── messaging/TransactionEvent.java      # Task 4
    │   ├── messaging/TransactionEventProducer.java # Task 4
    │   ├── web/dto/CreateTransactionRequest.java# Task 5
    │   ├── web/dto/TransactionResponse.java     # Task 5
    │   ├── service/TransactionService.java      # Task 5
    │   ├── service/AccountNotFoundException.java# Task 5
    │   ├── web/TransactionController.java        # Task 6
    │   └── web/ApiExceptionHandler.java          # Task 6
    ├── src/main/resources/application.yml        # Task 2 (created), Task 4/5 (extended)
    ├── src/main/resources/db/migration/V1__init.sql # Task 3
    └── src/test/java/com/fraudshield/transaction/
        ├── service/TransactionServiceTest.java   # Task 5
        └── web/TransactionControllerTest.java    # Task 6
```

**Task dependency order:** 1 (infra) → 2 (project skeleton) → 3 (persistence) → 4 (messaging) → 5 (service) → 6 (web) → 7 (wrapper + run docs) → 8 (ADRs). Tasks 1 and 8 are independent of the Java build and could be done any time, but are placed where they read best.

---

### Task 1: docker-compose infrastructure (Postgres + Kafka KRaft)

**Files:**
- Create: `docker-compose.yml`
- Create: `infra/postgres/init/01-create-databases.sql`

**Interfaces:**
- Produces: Postgres on `localhost:5432` (user/pass/db = `fraudshield`/`fraudshield`/`transaction_db`, plus a `fraud_db`); Kafka broker on `localhost:9092`. Task 2's `application.yml` consumes these endpoints.

> **Note on verification:** Docker is not installed on this machine, so the agentic worker CANNOT run `docker compose up`. This task's deliverables are the two files; they are reviewed by inspection. The runtime check is a **manual user step** recorded at the end of the task.

- [ ] **Step 1: Create the Postgres init script**

Create `infra/postgres/init/01-create-databases.sql`:

```sql
-- Runs ONLY on first initialization of an empty Postgres data directory.
-- POSTGRES_DB=transaction_db is created automatically by the image; create the second DB here.
CREATE DATABASE fraud_db;
```

- [ ] **Step 2: Create docker-compose.yml**

Create `docker-compose.yml`:

```yaml
name: fraudshield

services:
  postgres:
    image: postgres:16-alpine
    container_name: fraudshield-postgres
    environment:
      POSTGRES_USER: fraudshield
      POSTGRES_PASSWORD: fraudshield
      POSTGRES_DB: transaction_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./infra/postgres/init:/docker-entrypoint-initdb.d:ro
    networks:
      - fraudshield-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U fraudshield -d transaction_db"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s

  kafka:
    image: apache/kafka:3.9.0
    container_name: fraudshield-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: "5L6g3nShT-eMCtK--X86sw"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks:
      - fraudshield-net
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 20s

volumes:
  pgdata:
  kafka-data:

networks:
  fraudshield-net:
    driver: bridge
```

- [ ] **Step 3: Lint the compose file syntax (no Docker required)**

The worker cannot run Docker. Verify YAML validity instead:

Run:
```powershell
powershell -NoProfile -Command "[void][System.Reflection.Assembly]::LoadWithPartialName('System.Web'); Get-Content docker-compose.yml -Raw | ForEach-Object { if ($_ -match 'services:' -and $_ -match 'volumes:' -and $_ -match 'kafka:' -and $_ -match 'postgres:') { 'compose file has required top-level keys' } else { throw 'compose file missing required keys' } }"
```
Expected: `compose file has required top-level keys`

- [ ] **Step 4: Commit**

```powershell
git add docker-compose.yml infra/postgres/init/01-create-databases.sql
git commit -m "feat(infra): add docker-compose for Postgres + Kafka KRaft"
```

- [ ] **Step 5: MANUAL USER STEP — runtime verification (after installing Docker Desktop)**

Tell the user to run, from the repo root:
```powershell
docker compose up -d
docker compose ps          # both services should show (healthy)
```
Expected: `fraudshield-postgres` and `fraudshield-kafka` both reach `healthy`. `docker compose down` keeps data; `docker compose down -v` clears the volumes.

---

### Task 2: Maven project skeleton + Spring Boot app

**Files:**
- Create: `transaction-service/pom.xml`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/TransactionServiceApplication.java`
- Create: `transaction-service/src/main/resources/application.yml`

**Interfaces:**
- Produces: a buildable Spring Boot module with package root `com.fraudshield.transaction`; `application.yml` defining port `8081`, datasource, JPA, Flyway, and Kafka config that later tasks extend.

- [ ] **Step 1: Create pom.xml**

Create `transaction-service/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.5</version>
        <relativePath/>
    </parent>

    <groupId>com.fraudshield</groupId>
    <artifactId>transaction-service</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>transaction-service</name>
    <description>FraudShield Transaction Service</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create the application main class**

Create `transaction-service/src/main/java/com/fraudshield/transaction/TransactionServiceApplication.java`:

```java
package com.fraudshield.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TransactionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionServiceApplication.class, args);
    }
}
```

- [ ] **Step 3: Create application.yml**

Create `transaction-service/src/main/resources/application.yml`:

```yaml
server:
  port: 8081

spring:
  application:
    name: transaction-service
  datasource:
    url: jdbc:postgresql://localhost:5432/transaction_db
    username: fraudshield
    password: fraudshield
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
    properties:
      hibernate:
        format_sql: true
  flyway:
    enabled: true
    locations: classpath:db/migration
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

management:
  endpoints:
    web:
      exposure:
        include: health,info

fraudshield:
  topics:
    transactions: transactions
```

- [ ] **Step 4: Verify the project compiles**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -DskipTests compile
```
Expected: BUILD SUCCESS (downloads dependencies on first run; no compile errors).

- [ ] **Step 5: Commit**

```powershell
git add transaction-service/pom.xml transaction-service/src/main/java/com/fraudshield/transaction/TransactionServiceApplication.java transaction-service/src/main/resources/application.yml
git commit -m "feat(transaction): scaffold Spring Boot module"
```

---

### Task 3: Persistence — entities, repositories, Flyway migration

**Files:**
- Create: `transaction-service/src/main/resources/db/migration/V1__init.sql`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/domain/Account.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/domain/Transaction.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/repository/AccountRepository.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/repository/TransactionRepository.java`

**Interfaces:**
- Produces:
  - `Account` entity: fields `id: UUID`, `customerId: String`, `homeCountry: String`, `status: String`, `avgAmount: BigDecimal`, `createdAt: Instant`.
  - `Transaction` entity: fields `id: UUID`, `accountId: UUID`, `amount: BigDecimal`, `currency: String`, `country: String`, `merchantId: String`, `merchantCategory: String`, `channel: String`, `deviceId: String`, `ip: String`, `occurredAt: Instant`, `createdAt: Instant`. Builder via Lombok `@Builder`.
  - `TransactionRepository extends JpaRepository<Transaction, UUID>`.
  - `AccountRepository extends JpaRepository<Account, UUID>` with `boolean existsById(UUID id)` (inherited).
  - A seeded account row with id `11111111-1111-1111-1111-111111111111`.

- [ ] **Step 1: Create the Flyway migration**

Create `transaction-service/src/main/resources/db/migration/V1__init.sql`:

```sql
CREATE TABLE accounts (
    id           UUID PRIMARY KEY,
    customer_id  VARCHAR(64)  NOT NULL,
    home_country VARCHAR(2)   NOT NULL,
    status       VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
    avg_amount   NUMERIC(15,2) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id                UUID PRIMARY KEY,
    account_id        UUID         NOT NULL REFERENCES accounts(id),
    amount            NUMERIC(15,2) NOT NULL,
    currency          VARCHAR(3)   NOT NULL,
    country           VARCHAR(2)   NOT NULL,
    merchant_id       VARCHAR(64),
    merchant_category VARCHAR(32),
    channel           VARCHAR(16)  NOT NULL,
    device_id         VARCHAR(64),
    ip                VARCHAR(45),
    occurred_at       TIMESTAMPTZ  NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_transactions_account_id ON transactions(account_id);

-- Seed one account so POST /transactions works out of the box.
INSERT INTO accounts (id, customer_id, home_country, status, avg_amount)
VALUES ('11111111-1111-1111-1111-111111111111', 'cust_0001', 'CH', 'ACTIVE', 200.00);
```

- [ ] **Step 2: Create the Account entity**

Create `transaction-service/src/main/java/com/fraudshield/transaction/domain/Account.java`:

```java
package com.fraudshield.transaction.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "accounts")
@Getter
@Setter
public class Account {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(name = "home_country", nullable = false)
    private String homeCountry;

    @Column(nullable = false)
    private String status;

    @Column(name = "avg_amount", nullable = false)
    private BigDecimal avgAmount;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private Instant createdAt;
}
```

- [ ] **Step 3: Create the Transaction entity**

Create `transaction-service/src/main/java/com/fraudshield/transaction/domain/Transaction.java`:

```java
package com.fraudshield.transaction.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "transactions")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    @Id
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Column(nullable = false)
    private String country;

    @Column(name = "merchant_id")
    private String merchantId;

    @Column(name = "merchant_category")
    private String merchantCategory;

    @Column(nullable = false)
    private String channel;

    @Column(name = "device_id")
    private String deviceId;

    @Column
    private String ip;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;
}
```

- [ ] **Step 4: Create the repositories**

Create `transaction-service/src/main/java/com/fraudshield/transaction/repository/AccountRepository.java`:

```java
package com.fraudshield.transaction.repository;

import com.fraudshield.transaction.domain.Account;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountRepository extends JpaRepository<Account, UUID> {
}
```

Create `transaction-service/src/main/java/com/fraudshield/transaction/repository/TransactionRepository.java`:

```java
package com.fraudshield.transaction.repository;

import com.fraudshield.transaction.domain.Transaction;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionRepository extends JpaRepository<Transaction, UUID> {
}
```

- [ ] **Step 5: Verify compilation**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -DskipTests compile
```
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```powershell
git add transaction-service/src/main/resources/db/migration/V1__init.sql transaction-service/src/main/java/com/fraudshield/transaction/domain transaction-service/src/main/java/com/fraudshield/transaction/repository
git commit -m "feat(transaction): add JPA entities, repositories, and V1 migration"
```

---

### Task 4: Messaging — event DTO + Kafka producer

**Files:**
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/messaging/TransactionEvent.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/messaging/TransactionEventProducer.java`

**Interfaces:**
- Consumes: `KafkaTemplate<String, Object>` (Spring Boot autoconfigured from `application.yml`), and the property `fraudshield.topics.transactions`.
- Produces:
  - `TransactionEvent` (Java record) with fields exactly: `UUID eventId`, `UUID transactionId`, `UUID accountId`, `BigDecimal amount`, `String currency`, `String country`, `String merchantId`, `String merchantCategory`, `String channel`, `Instant occurredAt`.
  - `TransactionEventProducer` with method `void publish(TransactionEvent event)` that sends to the `transactions` topic with key = `event.accountId().toString()`.

- [ ] **Step 1: Create the event record**

Create `transaction-service/src/main/java/com/fraudshield/transaction/messaging/TransactionEvent.java`:

```java
package com.fraudshield.transaction.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Value published on the `transactions` topic. Fields match docs/architecture.md §4.
 */
public record TransactionEvent(
        UUID eventId,
        UUID transactionId,
        UUID accountId,
        BigDecimal amount,
        String currency,
        String country,
        String merchantId,
        String merchantCategory,
        String channel,
        Instant occurredAt
) {
}
```

- [ ] **Step 2: Create the producer**

Create `transaction-service/src/main/java/com/fraudshield/transaction/messaging/TransactionEventProducer.java`:

```java
package com.fraudshield.transaction.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransactionEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public TransactionEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                    @Value("${fraudshield.topics.transactions}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Publishes keyed by accountId so all of one account's events land in the same
     * partition and stay ordered (see docs/architecture.md §4).
     */
    public void publish(TransactionEvent event) {
        kafkaTemplate.send(topic, event.accountId().toString(), event);
    }
}
```

- [ ] **Step 3: Verify compilation**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -DskipTests compile
```
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```powershell
git add transaction-service/src/main/java/com/fraudshield/transaction/messaging
git commit -m "feat(transaction): add TransactionEvent and Kafka producer"
```

---

### Task 5: Service layer (save + publish) with TDD

**Files:**
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/web/dto/CreateTransactionRequest.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/web/dto/TransactionResponse.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/service/AccountNotFoundException.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/service/TransactionService.java`
- Test: `transaction-service/src/test/java/com/fraudshield/transaction/service/TransactionServiceTest.java`

**Interfaces:**
- Consumes: `TransactionRepository`, `AccountRepository`, `TransactionEventProducer` (Task 3, 4).
- Produces:
  - `CreateTransactionRequest` (record): `UUID accountId` (`@NotNull`), `BigDecimal amount` (`@NotNull @Positive`), `String currency` (`@NotBlank`), `String country` (`@NotBlank`), `String merchantId`, `String merchantCategory`, `String channel` (`@NotBlank`), `String deviceId`, `String ip`, `Instant occurredAt` (`@NotNull`).
  - `TransactionResponse` (record): `UUID id`, `UUID accountId`, `BigDecimal amount`, `String currency`, `String country`, `String channel`, `Instant occurredAt`.
  - `AccountNotFoundException extends RuntimeException`.
  - `TransactionService.record(CreateTransactionRequest request): TransactionResponse` — throws `AccountNotFoundException` if `accountId` is unknown; otherwise saves a `Transaction` (generating `id`) and calls `producer.publish(...)` with a `TransactionEvent` mapped from the saved entity, with a fresh `eventId`.

- [ ] **Step 1: Create the DTOs**

Create `transaction-service/src/main/java/com/fraudshield/transaction/web/dto/CreateTransactionRequest.java`:

```java
package com.fraudshield.transaction.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record CreateTransactionRequest(
        @NotNull UUID accountId,
        @NotNull @Positive BigDecimal amount,
        @NotBlank String currency,
        @NotBlank String country,
        String merchantId,
        String merchantCategory,
        @NotBlank String channel,
        String deviceId,
        String ip,
        @NotNull Instant occurredAt
) {
}
```

Create `transaction-service/src/main/java/com/fraudshield/transaction/web/dto/TransactionResponse.java`:

```java
package com.fraudshield.transaction.web.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionResponse(
        UUID id,
        UUID accountId,
        BigDecimal amount,
        String currency,
        String country,
        String channel,
        Instant occurredAt
) {
}
```

Create `transaction-service/src/main/java/com/fraudshield/transaction/service/AccountNotFoundException.java`:

```java
package com.fraudshield.transaction.service;

import java.util.UUID;

public class AccountNotFoundException extends RuntimeException {
    public AccountNotFoundException(UUID accountId) {
        super("Account not found: " + accountId);
    }
}
```

- [ ] **Step 2: Write the failing test**

Create `transaction-service/src/test/java/com/fraudshield/transaction/service/TransactionServiceTest.java`:

```java
package com.fraudshield.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fraudshield.transaction.domain.Transaction;
import com.fraudshield.transaction.messaging.TransactionEvent;
import com.fraudshield.transaction.messaging.TransactionEventProducer;
import com.fraudshield.transaction.repository.AccountRepository;
import com.fraudshield.transaction.repository.TransactionRepository;
import com.fraudshield.transaction.web.dto.CreateTransactionRequest;
import com.fraudshield.transaction.web.dto.TransactionResponse;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    @Mock TransactionRepository transactionRepository;
    @Mock AccountRepository accountRepository;
    @Mock TransactionEventProducer producer;
    @InjectMocks TransactionService service;

    private static final UUID ACCOUNT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private CreateTransactionRequest validRequest() {
        return new CreateTransactionRequest(
                ACCOUNT_ID, new BigDecimal("1299.00"), "USD", "FR",
                "m_123", "ELECTRONICS", "ONLINE", "dev_1", "1.2.3.4",
                Instant.parse("2026-06-22T10:15:30Z"));
    }

    @Test
    void recordPersistsAndPublishesKeyedByAccountId() {
        when(accountRepository.existsById(ACCOUNT_ID)).thenReturn(true);
        when(transactionRepository.save(any(Transaction.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        TransactionResponse response = service.record(validRequest());

        assertThat(response.accountId()).isEqualTo(ACCOUNT_ID);
        assertThat(response.id()).isNotNull();
        assertThat(response.amount()).isEqualByComparingTo("1299.00");

        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(producer).publish(eventCaptor.capture());
        TransactionEvent event = eventCaptor.getValue();
        assertThat(event.accountId()).isEqualTo(ACCOUNT_ID);
        assertThat(event.transactionId()).isEqualTo(response.id());
        assertThat(event.eventId()).isNotNull();
        assertThat(event.currency()).isEqualTo("USD");
    }

    @Test
    void recordReturnsResponseEvenWhenPublishFails() {
        // Phase-1 dual-write: a publish failure must NOT roll back the saved row
        // (see spec §4.7 / ADR 0002). The transaction is still recorded and returned.
        when(accountRepository.existsById(ACCOUNT_ID)).thenReturn(true);
        when(transactionRepository.save(any(Transaction.class)))
                .thenAnswer(inv -> inv.getArgument(0));
        org.mockito.Mockito.doThrow(new RuntimeException("kafka down"))
                .when(producer).publish(any());

        TransactionResponse response = service.record(validRequest());

        assertThat(response.id()).isNotNull();
        assertThat(response.accountId()).isEqualTo(ACCOUNT_ID);
    }

    @Test
    void recordThrowsAndDoesNotPublishWhenAccountUnknown() {
        when(accountRepository.existsById(ACCOUNT_ID)).thenReturn(false);

        assertThatThrownBy(() -> service.record(validRequest()))
                .isInstanceOf(AccountNotFoundException.class);

        verify(transactionRepository, never()).save(any());
        verify(producer, never()).publish(any());
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -Dtest=TransactionServiceTest test
```
Expected: FAIL — compilation error, `TransactionService` does not exist yet (or symbol `record` not found).

- [ ] **Step 4: Implement the service**

Create `transaction-service/src/main/java/com/fraudshield/transaction/service/TransactionService.java`:

```java
package com.fraudshield.transaction.service;

import com.fraudshield.transaction.domain.Transaction;
import com.fraudshield.transaction.messaging.TransactionEvent;
import com.fraudshield.transaction.messaging.TransactionEventProducer;
import com.fraudshield.transaction.repository.AccountRepository;
import com.fraudshield.transaction.repository.TransactionRepository;
import com.fraudshield.transaction.web.dto.CreateTransactionRequest;
import com.fraudshield.transaction.web.dto.TransactionResponse;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;
    private final TransactionEventProducer producer;

    public TransactionService(TransactionRepository transactionRepository,
                              AccountRepository accountRepository,
                              TransactionEventProducer producer) {
        this.transactionRepository = transactionRepository;
        this.accountRepository = accountRepository;
        this.producer = producer;
    }

    @Transactional
    public TransactionResponse record(CreateTransactionRequest request) {
        if (!accountRepository.existsById(request.accountId())) {
            throw new AccountNotFoundException(request.accountId());
        }

        Transaction saved = transactionRepository.save(Transaction.builder()
                .id(UUID.randomUUID())
                .accountId(request.accountId())
                .amount(request.amount())
                .currency(request.currency())
                .country(request.country())
                .merchantId(request.merchantId())
                .merchantCategory(request.merchantCategory())
                .channel(request.channel())
                .deviceId(request.deviceId())
                .ip(request.ip())
                .occurredAt(request.occurredAt())
                .build());

        // TODO Phase 2: replace this direct publish with the outbox pattern
        // (write the event row in the same DB transaction; a relay publishes to Kafka).
        // Phase-1 accepted dual-write gap (ADR 0002): a publish failure is logged but
        // must NOT roll back the committed payment, so we swallow it here.
        try {
            producer.publish(new TransactionEvent(
                    UUID.randomUUID(),
                    saved.getId(),
                    saved.getAccountId(),
                    saved.getAmount(),
                    saved.getCurrency(),
                    saved.getCountry(),
                    saved.getMerchantId(),
                    saved.getMerchantCategory(),
                    saved.getChannel(),
                    saved.getOccurredAt()));
        } catch (RuntimeException ex) {
            log.error("Failed to publish TransactionEvent for transactionId={} (payment is saved; "
                    + "event lost until outbox in Phase 2)", saved.getId(), ex);
        }

        return new TransactionResponse(
                saved.getId(),
                saved.getAccountId(),
                saved.getAmount(),
                saved.getCurrency(),
                saved.getCountry(),
                saved.getChannel(),
                saved.getOccurredAt());
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -Dtest=TransactionServiceTest test
```
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```powershell
git add transaction-service/src/main/java/com/fraudshield/transaction/web/dto transaction-service/src/main/java/com/fraudshield/transaction/service transaction-service/src/test/java/com/fraudshield/transaction/service
git commit -m "feat(transaction): add service layer (save + publish) with unit tests"
```

---

### Task 6: Web layer (controller + error handling) with TDD

**Files:**
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/web/TransactionController.java`
- Create: `transaction-service/src/main/java/com/fraudshield/transaction/web/ApiExceptionHandler.java`
- Test: `transaction-service/src/test/java/com/fraudshield/transaction/web/TransactionControllerTest.java`

**Interfaces:**
- Consumes: `TransactionService.record(CreateTransactionRequest): TransactionResponse`, `AccountNotFoundException`.
- Produces: `POST /transactions` → `201 Created` + `TransactionResponse`; validation failure → `400`; `AccountNotFoundException` → `422`.

- [ ] **Step 1: Write the failing controller test**

Create `transaction-service/src/test/java/com/fraudshield/transaction/web/TransactionControllerTest.java`:

```java
package com.fraudshield.transaction.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudshield.transaction.service.AccountNotFoundException;
import com.fraudshield.transaction.service.TransactionService;
import com.fraudshield.transaction.web.dto.TransactionResponse;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(TransactionController.class)
class TransactionControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;
    @MockBean TransactionService transactionService;

    private static final UUID ACCOUNT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private String validBody() throws Exception {
        return objectMapper.writeValueAsString(java.util.Map.of(
                "accountId", ACCOUNT_ID.toString(),
                "amount", "1299.00",
                "currency", "USD",
                "country", "FR",
                "channel", "ONLINE",
                "occurredAt", "2026-06-22T10:15:30Z"));
    }

    @Test
    void postReturns201WhenValid() throws Exception {
        UUID id = UUID.randomUUID();
        when(transactionService.record(any())).thenReturn(new TransactionResponse(
                id, ACCOUNT_ID, new BigDecimal("1299.00"), "USD", "FR", "ONLINE",
                Instant.parse("2026-06-22T10:15:30Z")));

        mockMvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.accountId").value(ACCOUNT_ID.toString()));
    }

    @Test
    void postReturns400WhenAmountMissing() throws Exception {
        String body = objectMapper.writeValueAsString(java.util.Map.of(
                "accountId", ACCOUNT_ID.toString(),
                "currency", "USD",
                "country", "FR",
                "channel", "ONLINE",
                "occurredAt", "2026-06-22T10:15:30Z"));

        mockMvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void postReturns422WhenAccountUnknown() throws Exception {
        when(transactionService.record(any())).thenThrow(new AccountNotFoundException(ACCOUNT_ID));

        mockMvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody()))
                .andExpect(status().isUnprocessableEntity());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -Dtest=TransactionControllerTest test
```
Expected: FAIL — `TransactionController` does not exist.

- [ ] **Step 3: Implement the controller**

Create `transaction-service/src/main/java/com/fraudshield/transaction/web/TransactionController.java`:

```java
package com.fraudshield.transaction.web;

import com.fraudshield.transaction.service.TransactionService;
import com.fraudshield.transaction.web.dto.CreateTransactionRequest;
import com.fraudshield.transaction.web.dto.TransactionResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> create(@Valid @RequestBody CreateTransactionRequest request) {
        TransactionResponse response = transactionService.record(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
```

- [ ] **Step 4: Implement the exception handler**

Create `transaction-service/src/main/java/com/fraudshield/transaction/web/ApiExceptionHandler.java`:

```java
package com.fraudshield.transaction.web;

import com.fraudshield.transaction.service.AccountNotFoundException;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(AccountNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleAccountNotFound(AccountNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("error", "ACCOUNT_NOT_FOUND", "message", ex.getMessage()));
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q -Dtest=TransactionControllerTest test
```
Expected: PASS (3 tests).

- [ ] **Step 6: Run the full test suite**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -q test
```
Expected: PASS (6 tests total: 3 service + 3 controller).

- [ ] **Step 7: Commit**

```powershell
git add transaction-service/src/main/java/com/fraudshield/transaction/web transaction-service/src/test/java/com/fraudshield/transaction/web
git commit -m "feat(transaction): add POST /transactions controller + error handling"
```

---

### Task 7: Maven wrapper + run instructions

**Files:**
- Create (via Maven): `transaction-service/mvnw`, `transaction-service/mvnw.cmd`, `transaction-service/.mvn/wrapper/maven-wrapper.properties`
- Modify: `CLAUDE.md` (Commands section — note the per-session JAVA_HOME)
- Create: `transaction-service/README.md`

**Interfaces:**
- Produces: a working `./mvnw` in `transaction-service/` (per CLAUDE.md's documented command).

- [ ] **Step 1: Generate the Maven wrapper**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml -N wrapper:wrapper
```
Expected: BUILD SUCCESS; `mvnw`, `mvnw.cmd`, and `.mvn/wrapper/maven-wrapper.properties` appear in `transaction-service/`.

- [ ] **Step 2: Create the service README**

Create `transaction-service/README.md`:

````markdown
# Transaction Service

Records payments to PostgreSQL and publishes a `TransactionEvent` to the Kafka
`transactions` topic (keyed by `accountId`). Port `8081`.

## Run

This machine keeps the global `JAVA_HOME` on Java 8 for another project, so set
JDK 21 **for the current shell only** before running Maven:

```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
```

Then, from the repo root, start infra and run the service:

```powershell
docker compose up -d          # Postgres + Kafka (requires Docker Desktop)
cd transaction-service
./mvnw spring-boot:run
```

## Try it

```powershell
curl -X POST http://localhost:8081/transactions `
  -H "Content-Type: application/json" `
  -d '{"accountId":"11111111-1111-1111-1111-111111111111","amount":1299.00,"currency":"USD","country":"FR","channel":"ONLINE","occurredAt":"2026-06-22T10:15:30Z"}'
```

Expect `201 Created`. Inspect the event:

```powershell
docker exec -it fraudshield-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 1
```

## Test

```powershell
./mvnw test
```
````

- [ ] **Step 3: Update CLAUDE.md Commands section**

In `CLAUDE.md`, replace the `./mvnw spring-boot:run` bullet under "## Commands" so it reads:

```markdown
- `./mvnw spring-boot:run` — run a service (from its folder). On this machine, first set JDK 21 for the shell only (global JAVA_HOME stays on Java 8 for another project): `$env:JAVA_HOME = "<path-to-your-jdk-21>"`
```

- [ ] **Step 4: Verify the wrapper runs**

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
cd transaction-service
./mvnw -q -DskipTests compile
cd ..
```
Expected: BUILD SUCCESS via the wrapper.

- [ ] **Step 5: Commit**

```powershell
git add transaction-service/mvnw transaction-service/mvnw.cmd transaction-service/.mvn transaction-service/README.md CLAUDE.md
git commit -m "chore(transaction): add Maven wrapper and run instructions"
```

---

### Task 8: ADRs for the Phase 1 decisions

**Files:**
- Create: `docs/adr/0001-kafka-kraft-single-node.md`
- Create: `docs/adr/0002-direct-publish-before-outbox.md`

**Interfaces:**
- Produces: two ADRs (context → decision → consequences) per CLAUDE.md's workflow rule.

- [ ] **Step 1: Write ADR 0001**

Create `docs/adr/0001-kafka-kraft-single-node.md`:

```markdown
# ADR 0001 — Kafka in KRaft single-node mode (apache/kafka image)

Status: Accepted · Date: 2026-06-30

## Context
FraudShield needs a Kafka broker locally for the `transactions` and
`fraud-decisions` topics. Historically Kafka required a separate Zookeeper
ensemble. We want the lightest setup that teaches Kafka configuration honestly.

## Decision
Run a single Kafka node in **KRaft mode** (no Zookeeper), using the official
**`apache/kafka`** image. One process plays both `broker` and `controller`
roles. Topics are auto-created on first use for now. A fixed `CLUSTER_ID` is
baked into docker-compose so the data volume stays valid across restarts.

We chose `apache/kafka` over `confluentinc/cp-kafka` because it is the official
Apache image, is Apache-2.0 throughout, is lighter, and maps `KAFKA_*`
environment variables straight onto `server.properties` without a vendor
conversion layer — better for learning.

## Consequences
- Simplest possible local Kafka; no Zookeeper to run or reason about.
- Single node = no real replication (`replication.factor=1`); fine for learning,
  not production.
- Confluent ecosystem tools (Schema Registry, ksqlDB, Control Center) are not
  available — not needed for this project.
- Switching images later means rewriting the broker config block, since the two
  images' environment conventions differ.
```

- [ ] **Step 2: Write ADR 0002**

Create `docs/adr/0002-direct-publish-before-outbox.md`:

```markdown
# ADR 0002 — Publish events directly (outbox deferred to Phase 2)

Status: Accepted (temporary) · Date: 2026-06-30

## Context
When the Transaction Service records a payment, it must also publish a
`TransactionEvent` to Kafka. The reliable approach is the **outbox pattern**:
write the event to an `outbox` table in the same DB transaction as the payment,
and let a relay publish it to Kafka. The build plan schedules the outbox for
Phase 2.

## Decision
For Phase 1, save the payment to PostgreSQL and then publish to Kafka
**directly** from the service, immediately after the DB commit. The code carries
a `// TODO Phase 2: replace with outbox` marker at the publish site.

## Consequences
- Simple and fast to build; unblocks the end-to-end Phase 1 flow.
- **Accepted risk — dual write:** if the DB commit succeeds but the Kafka publish
  fails (or the process dies in between), the payment exists with no event, and
  downstream fraud scoring never sees it. We accept this gap for Phase 1.
- Phase 2 closes the gap by introducing the outbox table + relay; this ADR will
  be superseded by the outbox ADR at that point.
```

- [ ] **Step 3: Commit**

```powershell
git add docs/adr/0001-kafka-kraft-single-node.md docs/adr/0002-direct-publish-before-outbox.md
git commit -m "docs(adr): record KRaft single-node and direct-publish decisions"
```

---

## Final verification (end of plan)

- [ ] **Full build + test** (automated, no Docker needed)

Run:
```powershell
$env:JAVA_HOME = "<path-to-your-jdk-21>"
mvn -f transaction-service/pom.xml clean test
```
Expected: BUILD SUCCESS, 6 tests pass.

- [ ] **MANUAL USER STEP — full end-to-end** (requires Docker Desktop installed)

From the repo root:
```powershell
docker compose up -d
$env:JAVA_HOME = "<path-to-your-jdk-21>"
cd transaction-service; ./mvnw spring-boot:run
```
Then POST a transaction (see `transaction-service/README.md`) and confirm: `201`,
a row in `transaction_db.transactions`, and a message on the `transactions` topic.

---

## Notes for the implementer

- **Spring Boot version:** the plan pins `3.3.5`. If Maven reports it cannot resolve that exact version, use the latest stable `3.3.x` the local repo can reach and keep `java.version` at `21`.
- **JsonSerializer type headers:** `spring.kafka.producer.value-serializer` is `JsonSerializer`; default type headers are fine for Phase 1 (no consumer is wired up yet — that is Fraud Scoring in a later task).
- **Do not** touch the global `JAVA_HOME`. Every Maven command must set `$env:JAVA_HOME` process-scoped first (it is in each step).
- **Publish vs. transaction boundary:** `record()` is `@Transactional`, so the JPA flush/commit happens as the method returns. The publish call sits inside the method but is wrapped in a `try/catch` that logs and swallows failures, so a Kafka error never rolls back the saved payment — matching spec §4.7 and ADR 0002. This is deliberately the simple Phase-1 behavior; Phase 2's outbox makes the publish part of the same committed unit of work properly.
```
