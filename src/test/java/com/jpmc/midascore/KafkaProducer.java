package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(
            @Value("${general.kafka-topic}") String topic,
            KafkaTemplate<String, Transaction> kafkaTemplate
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        // input format from test looks like: "6, 7, 122.86"
        String[] parts = transactionLine.split(",");
        long senderId = Long.parseLong(parts[0].trim());
        long recipientId = Long.parseLong(parts[1].trim());
        float amount = Float.parseFloat(parts[2].trim());

        Transaction tx = new Transaction(senderId, recipientId, amount);
        kafkaTemplate.send(topic, tx);
    }
}
