package com.fraudshield.transaction.web;

import com.fraudshield.transaction.service.TransactionService;
import com.fraudshield.transaction.web.dto.CreateTransactionRequest;
import com.fraudshield.transaction.web.dto.TransactionResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> create(@Valid @RequestBody CreateTransactionRequest request) {
        TransactionResponse response = transactionService.record(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}