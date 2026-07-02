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
