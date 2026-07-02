# Design — Phase 1 Infrastructure & Transaction Service

*Date: 2026-06-30 · Scope: FraudShield Phase 1 (Day 1–2) · Status: Approved for planning*

## 1. Goal

Stand up the minimum runnable foundation for FraudShield:

1. A `docker-compose.yml` at the repo root that brings up **PostgreSQL** and **Kafka (KRaft mode)** with persistent volumes and healthchecks.
2. A scaffolded **Transaction Service** that records a payment to PostgreSQL and publishes a `TransactionEvent` to the `transactions` Kafka topic via `POST /transactions`.

This matches CLAUDE.md "Current status — Phase 1 (core), Day 1–2" and the ports/topics defined in `docs/architecture.md`. It deliberately stops short of the outbox pattern, idempotency, and the other services (those are Phase 2+).

### Non-goals (out of scope for this phase)

- Outbox pattern and the relay (Phase 2). We publish directly from the service with a `TODO` marker.
- Idempotent consumers / `processed_events` (Phase 2).
- MongoDB, Zipkin, the other four services (later phases).
- Testcontainers-based integration tests (Week 6). Tests here use mocks / slice tests.
- Authentication, rate limiting, the API Gateway (later).

## 2. Architecture decisions for this phase

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Kafka image | `apache/kafka` (KRaft, single node) | Official Apache image, Apache-2.0, no Zookeeper, clean `KAFKA_*` → `server.properties` mapping. Right fit for learning Kafka config directly. Candidate ADR. |
| PostgreSQL layout | One container, two databases (`transaction_db`, `fraud_db`) created by an init script | Honors database-per-service *logically* while staying light on a local machine. Both services keep separate schemas/connections. |
| Topic creation | Spring auto-creates `transactions` on first publish | Simplest thing that works for Phase 1; avoids hand-managing topics. Candidate ADR if we later switch to explicit topic provisioning. |
| Event publishing | Save to Postgres, then publish directly to Kafka inside the service, with a `// TODO Phase 2: replace with outbox` marker | CLAUDE.md schedules the outbox for Phase 2. This is the simplest correct Phase-1 step. Candidate ADR (records the known dual-write risk we accept temporarily). |
| Build tool | Maven, Java 21, Spring Boot 3.x | Per CLAUDE.md tech stack. |
| Boilerplate reduction | Lombok | Per CLAUDE.md (optional, accepted). |

> **Accepted risk (documented, not hidden):** publishing directly after the DB commit is a dual-write — if the publish fails after the row is committed, the event is lost. This is acceptable for Phase 1 and is exactly the problem the outbox pattern solves in Phase 2. The ADR will record this.

## 3. Component 1 — `docker-compose.yml`

Located at the repo root. Two infrastructure services.

### 3.1 `postgres`

- Image: `postgres:16-alpine`
- Ports: `5432:5432`
- Environment: `POSTGRES_USER=fraudshield`, `POSTGRES_PASSWORD=fraudshield`, `POSTGRES_DB=transaction_db`
- Named volume: `pgdata` → `/var/lib/postgresql/data` (data survives `down`/restart; removed only with `down -v`)
- Init script: `infra/postgres/init/01-create-databases.sql` mounted read-only into `/docker-entrypoint-initdb.d/`. Creates `fraud_db` (the default `POSTGRES_DB` already creates `transaction_db`). The init scripts run **only on first initialization of an empty data directory**.
- Healthcheck: `pg_isready -U fraudshield -d transaction_db` with sensible `interval`/`timeout`/`retries`/`start_period`.

### 3.2 `kafka`

- Image: `apache/kafka:3.9.0`
- Ports: `9092:9092` (host access)
- KRaft single node: one process acting as combined `broker,controller`.
- Listeners:
  - `PLAINTEXT` on `9092` advertised as `localhost:9092` for host clients.
  - `CONTROLLER` listener on `9093` (internal to KRaft quorum).
  - (Container-to-container access is not needed in Phase 1 because the Transaction Service still runs on the host via `mvn spring-boot:run`. We will add an internal `19092` listener when services are dockerized in a later phase.)
- Fixed `CLUSTER_ID` (a constant UUID baked into compose) so the volume stays valid across restarts.
- Named volume: `kafka-data` → `/var/lib/kafka/data`
- Healthcheck: `kafka-topics.sh --bootstrap-server localhost:9092 --list` (or the image's equivalent CLI path) with `start_period` generous enough for KRaft startup.

### 3.3 Volumes & networks

- Top-level named volumes: `pgdata`, `kafka-data`.
- A single user-defined bridge network (`fraudshield-net`) so future services can join by name.

### 3.4 Verification for this component

`docker compose up -d` → both containers reach `healthy`. `docker compose down` keeps data; `docker compose down -v` clears it.

## 4. Component 2 — `transaction-service/`

Maven module under the repo root. Package root `com.fraudshield.transaction`.

### 4.1 Directory layout

```
transaction-service/
├── pom.xml
├── src/main/java/com/fraudshield/transaction/
│   ├── TransactionServiceApplication.java
│   ├── domain/
│   │   ├── Account.java                 # JPA entity (read-side for now; seeded via migration)
│   │   └── Transaction.java             # JPA entity
│   ├── repository/
│   │   ├── AccountRepository.java
│   │   └── TransactionRepository.java
│   ├── web/
│   │   ├── TransactionController.java    # POST /transactions, GET /transactions/{id}
│   │   └── dto/
│   │       ├── CreateTransactionRequest.java
│   │       └── TransactionResponse.java
│   ├── service/
│   │   └── TransactionService.java       # save + publish
│   └── messaging/
│       ├── TransactionEvent.java         # event payload (matches architecture.md §4)
│       └── TransactionEventProducer.java # KafkaTemplate wrapper, key = accountId
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/
│       └── V1__init.sql                  # Flyway: accounts + transactions tables (+ a seed account)
└── src/test/java/com/fraudshield/transaction/
    ├── web/TransactionControllerTest.java
    └── service/TransactionServiceTest.java
```

### 4.2 `pom.xml` dependencies

Spring Boot 3.x parent. Starters: `spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-boot-starter-validation`, `spring-boot-starter-actuator`, `spring-kafka`, `flyway-core`, `postgresql` (runtime), `lombok` (optional/provided). Test: `spring-boot-starter-test`, `spring-kafka-test`.

### 4.3 Data model (Flyway `V1__init.sql`)

Matches `docs/architecture.md §5` (Transaction Service / PostgreSQL), minus the `outbox` table (Phase 2).

- **accounts**: `id` (UUID PK), `customer_id`, `home_country`, `status`, `avg_amount` (numeric), `created_at`
- **transactions**: `id` (UUID PK), `account_id` (FK → accounts), `amount`, `currency`, `country`, `merchant_id`, `merchant_category`, `channel`, `device_id`, `ip`, `occurred_at`, `created_at`
- A small **seed**: one account so `POST /transactions` works out of the box and the `accountId` key has a real target.

> `outbox` is intentionally omitted here and added in Phase 2 with its own migration (`V2__outbox.sql`).

### 4.4 Event schema — `TransactionEvent`

Serialized as JSON onto the `transactions` topic, **keyed by `accountId`** (keeps an account's events in one partition → ordering, as architecture.md §4 explains). Fields exactly per architecture.md §4:

`eventId` (UUID), `transactionId` (UUID), `accountId` (UUID), `amount`, `currency`, `country`, `merchantId`, `merchantCategory`, `channel`, `occurredAt` (Instant).

### 4.5 Request flow — `POST /transactions`

1. Controller receives `CreateTransactionRequest`; `@Valid` enforces required fields and constraints.
2. `TransactionService`:
   a. Builds and saves a `Transaction` row to `transaction_db`.
   b. Maps it to a `TransactionEvent` and calls `TransactionEventProducer.publish(event)` with key = `accountId`. `// TODO Phase 2: replace direct publish with the outbox pattern.`
3. Controller returns `201 Created` with `TransactionResponse` (the persisted id + echoed fields).

`GET /transactions/{id}` returns the stored transaction or `404`.

`/actuator/health` is exposed.

### 4.6 `application.yml`

- `server.port: 8081`
- Datasource → `jdbc:postgresql://localhost:5432/transaction_db`, user/pass `fraudshield`
- `spring.jpa.hibernate.ddl-auto: validate` (Flyway owns the schema)
- Flyway enabled, locations `classpath:db/migration`
- Kafka: `bootstrap-servers: localhost:9092`, JSON value serializer, string key serializer
- A `fraudshield.topics.transactions: transactions` property the producer reads (no hard-coded topic name in code)

### 4.7 Error handling

- Validation failures → `400` with a clear body (Spring's default validation error structure is acceptable for Phase 1).
- Unknown `accountId` (no matching account) → `422 Unprocessable Entity`. The request is well-formed but references a non-existent account, so the service rejects it rather than persisting an orphan transaction. (`404` is reserved for "this URL/resource doesn't exist", e.g. `GET /transactions/{id}` on a missing id.)
- Kafka publish failure after a committed row → logged as an error (the documented Phase-1 dual-write gap). It does not roll back the saved transaction.

### 4.8 Tests

- `TransactionControllerTest` (`@WebMvcTest`): valid request → `201` + correct body; invalid request → `400`; service layer mocked.
- `TransactionServiceTest` (plain unit test, Mockito): saving persists via `TransactionRepository` and calls the producer **with key = accountId** and a correctly mapped event.
- No Kafka broker or Postgres is started in tests for this phase (Testcontainers is Week 6). The producer is mocked.

## 5. What the user runs afterward

After the files exist, generate the Maven wrapper so `./mvnw` (per CLAUDE.md) works:

```bash
cd transaction-service
mvn -N wrapper:wrapper
```

(We do not commit a hand-made wrapper jar.) Then:

```bash
docker compose up -d                 # from repo root: postgres + kafka
cd transaction-service && ./mvnw spring-boot:run
```

## 6. ADRs to write (during planning/implementation)

Per CLAUDE.md, capture the major Phase-1 decisions as short ADRs in `docs/adr/` (context → decision → consequences):

1. **Kafka in KRaft single-node mode** (no Zookeeper) and choice of the `apache/kafka` image.
2. **Direct publish before the outbox** — records the accepted dual-write risk and that Phase 2 replaces it with the outbox pattern.

(One container / two databases can be folded into the existing database-per-service ADR or noted in the Kafka/infra ADR — decided in planning.)

## 7. Verification summary

- `docker compose up -d` → `postgres` and `kafka` both `healthy`.
- `transaction-service` starts on `8081`; Flyway applies `V1`; `/actuator/health` is `UP`.
- `POST /transactions` with the seeded `accountId` → `201`, a row in `transaction_db.transactions`, and a message on the `transactions` topic keyed by `accountId` (verifiable with `kafka-console-consumer`).
- `./mvnw test` passes.
