# FraudShield — Claude Code Project Context

## What this is
FraudShield is a **learning pet project**: a near-real-time, post-authorization fraud monitoring system built with event-driven microservices.

## Architecture (WHAT)
Event-driven microservices around a Kafka event log:
- **API Gateway** (:8080) — routing, JWT auth, rate limiting (Spring Cloud Gateway)
- **Transaction Service** (:8081) — records payments to PostgreSQL, publishes events via the outbox pattern
- **Fraud Scoring** (:8082) — consumes `transactions`, applies rules, idempotent, emits `fraud-decisions`
- **Fraud Report** (:8083) — consumes `fraud-decisions`, MongoDB read model (CQRS), analyst queries
- **Notification** (:8084) — consumes `fraud-decisions`, sends/logs alerts

Kafka topics: `transactions` (key=`accountId`), `fraud-decisions` (key=`transactionId`), `transactions.DLT`.
Infra (docker-compose): PostgreSQL 5432, MongoDB 27017, Kafka KRaft 9092, Zipkin 9411.

## Tech stack
Java 21, Spring Boot 3.x; Spring Web, Data JPA, Data MongoDB, Spring for Apache Kafka, Cloud Gateway, OpenFeign, Resilience4j; Flyway; Micrometer Tracing + Zipkin; Testcontainers. Build: Maven. Mono-repo, one folder per service.

## Commands
- `docker-compose up -d` — start infra (and services once dockerized)
- `./mvnw spring-boot:run` — run a service (from its folder). On this machine, first set JDK 21 for the shell only (global JAVA_HOME stays on Java 8 for another project): `$env:JAVA_HOME = "C:\Users\NGOA\AppData\Local\Programs\Eclipse Adoptium\jdk-21.0.9.10-hotspot"`
- `./mvnw test` — run tests
  *(Keep this list updated as the project grows.)*

## Key decisions — don't change without a reason (see docs/adr/)
- **Database-per-service.** No shared DB.
- **Outbox pattern** for reliable publishing: payment + event written in one DB transaction; a relay publishes to Kafka.
- **Idempotent consumers via the database** (upsert by `transactionId` / `processed_events` table). **Not Redis** — Redis is only a future scaling option.
- **CQRS:** write = PostgreSQL, read = MongoDB, kept current by consuming events; eventual consistency accepted.
- **Post-authorization monitoring** (async scoring), not inline blocking.

## Conventions & workflow
- This is a learning project — prefer the simplest thing that works; don't over-engineer or add production complexity unless asked.
- Keep fraud rules trivial (3–4 simple rules). The architecture is the point, not the detection algorithm.
- Plan before coding (use Plan Mode for multi-file work); make the smallest coherent change.
- Never refactor unrelated code unless explicitly asked; respect existing patterns.
- Write/extend tests as part of each change.
- For each major pattern, help me write a short **ADR** (context → decision → consequences) in `docs/adr/`.

## Reference docs (read for detail; do not duplicate here)
- `docs/business-overview.md` — domain, rules, false-positive vs. false-negative trade-off
- `docs/architecture.md` — services, data models, event schemas, endpoints, ports
- `docs/` — build schedule and roadmap

## Current status
**Phase 1 — DONE.** Transaction Service is built and on `main`: `POST /transactions` validates input, checks the account exists (422 if not), saves to PostgreSQL (Flyway V1 + seed account), and publishes a `TransactionEvent` to the `transactions` topic keyed by `accountId`, returning 201. 6 unit/slice tests (TDD). docker-compose (Postgres + Kafka KRaft) with volumes + healthchecks. Maven wrapper. ADRs 0001 (KRaft) + 0002 (direct-publish-before-outbox — accepted dual-write gap). DB credentials come from `.env` (git-ignored); secret scanning via gitleaks + pre-commit.

**Next — Phase 2 (Kafka backbone):** Fraud Scoring service — consume `transactions`, apply 3–4 simple rules (LARGE_AMOUNT, VELOCITY, COUNTRY_MISMATCH…), emit `fraud-decisions`. Reads account data from Transaction Service via Feign.

**Then — Phase 3 (reliability):** the patterns that make it trustworthy — **outbox + relay** (closes the Phase 1 dual-write gap), **idempotent consumer** (`processed_events`, upsert by `eventId` — not Redis), **dead-letter topic** (`transactions.DLT`) + retries for poison messages.

> **Learning mode:** from Phase 2 the USER writes the code (especially the Phase 3 reliability patterns — the high interview-value ones); Claude acts as interviewer/reviewer — state requirements, write failing tests, hint (don't hand answers), then grill. See the interview-prep memory.
