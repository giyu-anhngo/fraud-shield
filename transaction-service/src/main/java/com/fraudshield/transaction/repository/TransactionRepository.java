package com.fraudshield.transaction.repository;

import com.fraudshield.transaction.domain.Transaction;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionRepository extends JpaRepository<Transaction, UUID> {
}
