package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class DatabaseConduit {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository,
                           TransactionRecordRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    // ✅ Keep this EXACT name/signature because existing code uses it
    public UserRecord save(UserRecord userRecord) {
        return userRepository.save(userRecord);
    }

    // ✅ Keep this too (you’ll use it in listener)
    public Optional<UserRecord> find(long id) {
    return Optional.ofNullable(userRepository.findById(id));
    }


    // ✅ For transaction records
    public TransactionRecord save(TransactionRecord tx) {
        return transactionRepository.save(tx);
    }
}
