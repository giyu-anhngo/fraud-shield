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