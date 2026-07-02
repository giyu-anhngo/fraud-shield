# FraudShield — Technical Architecture (Detailed Design)

*The implementable design: services, data models, events, endpoints, ports, and repo layout. Pairs with `fraudshield-business-overview.md` (what & why) and the 6-week build plan (when).*

---

## 1. System overview

FraudShield is an **event-driven microservices** system. Payments enter through a gateway, are recorded by the Transaction Service, and flow through **Apache Kafka** to a Fraud Scoring service that decides whether each one is suspicious. Decisions fan out over Kafka to a Fraud Report read model (for analysts) and a Notification service (for alerts).

Communication is **synchronous** (REST/Feign) where a caller needs an immediate answer, and **asynchronous** (Kafka events) for everything that should be decoupled and resilient.

---

## 2. Services

| Service | Responsibility | Stack | Port | Database |
|---------|----------------|-------|------|----------|
| **API Gateway** | Single entry point: routing, JWT auth, rate limiting | Spring Cloud Gateway | 8080 | — |
| **Transaction Service** | Records payments (source of truth), owns account data, publishes events via outbox | Spring Boot, JPA | 8081 | PostgreSQL |
| **Fraud Scoring** | Consumes transactions, applies rules, idempotent, emits decisions | Spring Boot, Spring Kafka | 8082 | PostgreSQL (own DB) |
| **Fraud Report** | Consumes decisions, builds the read model, serves analyst queries (CQRS read side) | Spring Boot, Spring Data Mongo | 8083 | MongoDB |
| **Notification** | Consumes decisions, sends/logs alerts | Spring Boot, Spring Kafka | 8084 | — |

> Account data lives in the Transaction Service for the MVP (avoids a sixth service). Fraud Scoring reads it via a synchronous Feign call — that call is where the circuit breaker goes.

---

## 3. Communication map

**Synchronous (REST / Feign — caller waits):**
- Client → Gateway → Transaction Service (`POST /transactions`)
- Client (analyst) → Gateway → Fraud Report (`GET /cases`)
- Fraud Scoring → Transaction Service (`GET /accounts/{id}`) via **Feign + Resilience4j circuit breaker**

**Asynchronous (Kafka events — fire and forget):**
- Transaction Service → `transactions` topic
- Fraud Scoring consumes `transactions`, produces `fraud-decisions`
- Fraud Report consumes `fraud-decisions`
- Notification consumes `fraud-decisions`
- Failed messages → `transactions.DLT`

---

## 4. Kafka topics & event schemas

| Topic | Key | Produced by | Consumed by |
|-------|-----|-------------|-------------|
| `transactions` | `accountId` | Transaction Service | Fraud Scoring |
| `fraud-decisions` | `transactionId` | Fraud Scoring | Fraud Report, Notification |
| `transactions.DLT` | `accountId` | Fraud Scoring (on repeated failure) | (manual inspection) |

> Keying `transactions` by `accountId` keeps all of one account's events in the same partition, so they stay **ordered** — important for velocity and impossible-travel rules.

**TransactionEvent** (value on `transactions`):
```json
{
  "eventId": "uuid",
  "transactionId": "uuid",
  "accountId": "uuid",
  "amount": 1299.00,
  "currency": "USD",
  "country": "FR",
  "merchantId": "m_123",
  "merchantCategory": "ELECTRONICS",
  "channel": "ONLINE",
  "occurredAt": "2026-06-22T10:15:30Z"
}
```

**FraudDecisionEvent** (value on `fraud-decisions`):
```json
{
  "eventId": "uuid",
  "transactionId": "uuid",
  "accountId": "uuid",
  "riskScore": 82,
  "decision": "SUSPICIOUS",
  "triggeredRules": ["LARGE_AMOUNT", "COUNTRY_MISMATCH"],
  "decidedAt": "2026-06-22T10:15:31Z"
}
```

`decision` is `NORMAL` or `SUSPICIOUS`. `eventId` exists so consumers can deduplicate.

---

## 5. Data models

### Transaction Service (PostgreSQL)
- **accounts**: `id`, `customer_id`, `home_country`, `status`, `avg_amount` (spending baseline), `created_at`
- **transactions**: `id`, `account_id`, `amount`, `currency`, `country`, `merchant_id`, `merchant_category`, `channel`, `device_id`, `ip`, `occurred_at`, `created_at`
- **outbox**: `id`, `aggregate_id`, `event_type`, `payload` (JSON), `created_at`, `published` (boolean) — the outbox row is written in the **same transaction** as the payment

### Fraud Scoring (PostgreSQL — its own database)
- **processed_events**: `event_id` (unique), `transaction_id`, `processed_at` — the idempotency / dedup table
- **fraud_decisions** (optional local copy): `transaction_id` (unique), `risk_score`, `decision`, `triggered_rules`, `decided_at`

### Fraud Report (MongoDB)
- collection **fraud_cases**: `caseId`, `transactionId`, `accountId`, `amount`, `country`, `riskScore`, `triggeredRules[]`, `status`, `createdAt`, `updatedAt`
- `status`: `OPEN → UNDER_REVIEW → CONFIRMED_FRAUD | CLEARED`

### Notification
- No database for the MVP (log alerts; optionally add a `sent_alerts` table later)

---

## 6. Fraud rules (Fraud Scoring)

Keep these simple. Each reads fields already on the event or the account.

| Rule code | Logic | Fields used |
|-----------|-------|-------------|
| `LARGE_AMOUNT` | `amount` > threshold OR > k × `avg_amount` | amount, account.avg_amount |
| `VELOCITY` | > N transactions for the account in M minutes | account_id, occurred_at |
| `IMPOSSIBLE_TRAVEL` | two transactions in distant countries within short time | country, occurred_at |
| `COUNTRY_MISMATCH` | `country` ≠ `account.home_country` | country, account.home_country |
| `BLACKLIST` | merchant/device/ip in a known-bad list | merchant_id, device_id, ip |

A transaction is `SUSPICIOUS` if its combined `riskScore` crosses a configurable threshold.

---

## 7. Endpoints

**API Gateway** (routes everything; applies JWT + rate limit)
- `/api/transactions/**` → Transaction Service
- `/api/cases/**` → Fraud Report

**Transaction Service**
- `POST /transactions` — record a payment, save + write outbox
- `GET /transactions/{id}`
- `GET /accounts/{id}` — used by Fraud Scoring via Feign

**Fraud Report**
- `GET /cases` — list/search (filters: `status`, `accountId`)
- `GET /cases/{id}`
- `PATCH /cases/{id}` — analyst sets status (review/resolve)

**Fraud Scoring / Notification** — no public REST; Kafka consumers + `/actuator/health` only.

---

## 8. Repository structure (mono-repo)

```
fraudshield/
├── docker-compose.yml
├── README.md
├── docs/
│   ├── business-overview.md
│   ├── build-plan.md
│   ├── architecture.md
│   └── adr/                 # one file per decision (Kafka, CQRS, outbox, JWT, idempotency)
├── api-gateway/
├── transaction-service/
├── fraud-scoring-service/
├── fraud-report-service/
├── notification-service/
└── common-events/           # optional shared event DTOs
```

> **`common-events` trade-off:** a shared module avoids duplicating the event classes, but couples the services to one library. Purists duplicate the DTOs to keep services fully independent. For a learning project a small shared module is fine — just be ready to explain the trade.

---

## 9. Infrastructure (docker-compose containers)

| Container | Port | Purpose |
|-----------|------|---------|
| postgres | 5432 | Transaction + Fraud Scoring databases |
| mongodb | 27017 | Fraud Report read model |
| kafka | 9092 | Event log (run in KRaft mode — no Zookeeper needed) |
| zipkin | 9411 | Distributed tracing (added Week 6) |
| api-gateway | 8080 | |
| transaction-service | 8081 | |
| fraud-scoring-service | 8082 | |
| fraud-report-service | 8083 | |
| notification-service | 8084 | |

---

## 10. Tech stack

- **Java** 21 (LTS) — 17 also fine
- **Spring Boot 3.x** with: Spring Web, Spring Data JPA, Spring Data MongoDB, Spring for Apache Kafka, Validation, Actuator
- **Spring Cloud**: Gateway, OpenFeign (use the Spring Cloud release train that matches your Boot version)
- **Resilience4j** — circuit breaker, retry, timeout
- **Flyway** — database migrations
- **Micrometer Tracing + Zipkin** — tracing (Week 6)
- **Testcontainers** — integration tests (Week 6)
- Optional: Lombok, MapStruct
- **Build:** Maven or Gradle

> Check current stable versions at `start.spring.io` when you scaffold each service — it picks compatible Spring Boot + Spring Cloud versions for you automatically.