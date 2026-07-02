# Transaction Service

Records payments to PostgreSQL and publishes a `TransactionEvent` to the Kafka
`transactions` topic (keyed by `accountId`). Port `8081`.

## Run

This machine keeps the global `JAVA_HOME` on Java 8 for another project, so set
JDK 21 **for the current shell only** before running Maven:

```powershell
$env:JAVA_HOME = "C:\Users\NGOA\AppData\Local\Programs\Eclipse Adoptium\jdk-21.0.9.10-hotspot"
```

Then, from the repo root, create your local `.env` (DB credentials — git-ignored,
see `.env.example` for the variable list), start infra, and run the service:

```powershell
cp .env.example .env          # then fill in the values (first time only)
docker compose up -d          # Postgres + Kafka (requires Docker Desktop)
cd transaction-service
./mvnw spring-boot:run
```

The service reads `SPRING_DATASOURCE_*` from the environment; when running from
a shell, load them from `.env` or set them for the session.

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
