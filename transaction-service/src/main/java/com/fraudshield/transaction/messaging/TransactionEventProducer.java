package com.fraudshield.transaction.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransactionEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public TransactionEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                    @Value("${fraudshield.topics.transactions}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Publishes keyed by accountId so all of one account's events land in the same
     * partition and stay ordered (see docs/architecture.md §4).
     */
    public void publish(TransactionEvent event) {
        kafkaTemplate.send(topic, event.accountId().toString(), event);
    }
}
