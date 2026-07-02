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