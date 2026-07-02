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