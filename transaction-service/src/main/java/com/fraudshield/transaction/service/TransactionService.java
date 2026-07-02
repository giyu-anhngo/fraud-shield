package com.fraudshield.transaction.service;

import com.fraudshield.transaction.domain.Transaction;
import com.fraudshield.transaction.messaging.TransactionEvent;
import com.fraudshield.transaction.messaging.TransactionEventProducer;
import com.fraudshield.transaction.repository.AccountRepository;
import com.fraudshield.transaction.repository.TransactionRepository;
import com.fraudshield.transaction.web.dto.CreateTransactionRequest;
import com.fraudshield.transaction.web.dto.TransactionResponse;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;
    private final TransactionEventProducer producer;

    public TransactionService(TransactionRepository transactionRepository,
                              AccountRepository accountRepository,
                              TransactionEventProducer producer) {
        this.transactionRepository = transactionRepository;
        this.accountRepository = accountRepository;
        this.producer = producer;
    }

    @Transactional
    public TransactionResponse record(CreateTransactionRequest request) {
        if (!accountRepository.existsById(request.accountId())) {
            throw new AccountNotFoundException(request.accountId());
        }

        Transaction saved = transactionRepository.save(Transaction.builder()
                .id(UUID.randomUUID())
                .accountId(request.accountId())
                .amount(request.amount())
                .currency(request.currency())
                .country(request.country())
                .merchantId(request.merchantId())
                .merchantCategory(request.merchantCategory())
                .channel(request.channel())
                .deviceId(request.deviceId())
                .ip(request.ip())
                .occurredAt(request.occurredAt())
                .build());

        // TODO Phase 2: replace this direct publish with the outbox pattern
        // (write the event row in the same DB transaction; a relay publishes to Kafka).
        // Phase-1 accepted dual-write gap (ADR 0002): a publish failure is logged but
        // must NOT roll back the committed payment, so we swallow it here.
        try {
            producer.publish(new TransactionEvent(
                    UUID.randomUUID(),
                    saved.getId(),
                    saved.getAccountId(),
                    saved.getAmount(),
                    saved.getCurrency(),
                    saved.getCountry(),
                    saved.getMerchantId(),
                    saved.getMerchantCategory(),
                    saved.getChannel(),
                    saved.getOccurredAt()));
        } catch (RuntimeException ex) {
            log.error("Failed to publish TransactionEvent for transactionId={} (payment is saved; "
                    + "event lost until outbox in Phase 2)", saved.getId(), ex);
        }

        return new TransactionResponse(
                saved.getId(),
                saved.getAccountId(),
                saved.getAmount(),
                saved.getCurrency(),
                saved.getCountry(),
                saved.getChannel(),
                saved.getOccurredAt());
    }
}
