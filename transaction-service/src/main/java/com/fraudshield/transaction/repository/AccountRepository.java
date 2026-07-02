package com.fraudshield.transaction.repository;

import com.fraudshield.transaction.domain.Account;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountRepository extends JpaRepository<Account, UUID> {
}
