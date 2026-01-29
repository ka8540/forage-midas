package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public TransactionListener(DatabaseConduit databaseConduit,
                               RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction tx) {

        Optional<UserRecord> senderOpt = databaseConduit.find(tx.getSenderId());
        Optional<UserRecord> recipientOpt = databaseConduit.find(tx.getRecipientId());

        // ❌ invalid users
        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) return;

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        // ❌ insufficient balance
        if (sender.getBalance() < tx.getAmount()) return;

        // ✅ call incentive API
        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                tx,
                Incentive.class
        );

        float incentiveAmount = incentive.getAmount();

        // ✅ balances
        sender.setBalance(sender.getBalance() - tx.getAmount());
        recipient.setBalance(recipient.getBalance() + tx.getAmount() + incentiveAmount);

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        databaseConduit.save(
            new TransactionRecord(sender, recipient, tx.getAmount(), incentiveAmount)
        );
    }
}

