package com.folypizza.canopy.repartition;

import java.util.Optional;
import java.util.UUID;

public interface RepartitionStateStore extends AutoCloseable {
    record Ownership(long epoch, int boundaryX, String committedTransactionId) {
        public Ownership {
            committedTransactionId = committedTransactionId == null ? "" : committedTransactionId;
        }
    }

    enum BeginResult { ACQUIRED, IDEMPOTENT, CONFLICT, STALE_EPOCH, UNKNOWN }
    enum CommitResult { COMMITTED, IDEMPOTENT, CONFLICT, STALE_EPOCH, UNKNOWN }

    Ownership loadOwnership();
    Optional<RepartitionTransaction> loadActive();
    BeginResult begin(RepartitionTransaction transaction);
    boolean save(RepartitionTransaction transaction);
    CommitResult commit(UUID transactionId, long expectedEpoch, long newEpoch, int newBoundaryX);
    boolean clear(UUID transactionId);

    @Override
    default void close() {}
}
