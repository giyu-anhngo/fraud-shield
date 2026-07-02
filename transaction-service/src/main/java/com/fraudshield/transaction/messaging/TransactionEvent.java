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