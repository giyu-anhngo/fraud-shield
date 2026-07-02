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