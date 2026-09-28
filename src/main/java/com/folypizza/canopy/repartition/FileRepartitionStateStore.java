package com.folypizza.canopy.repartition;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.UUID;

/**
 * Restart-durable single-process state store. It deliberately provides weaker cluster-wide
 * guarantees than Redis: it prevents local concurrent transactions but cannot arbitrate two
 * independent shard processes during a network partition.
 */
public final class FileRepartitionStateStore implements RepartitionStateStore {
    private record Persisted(Ownership ownership, RepartitionTransaction active) {}

    private static final ObjectMapper MAPPER = RepartitionJsonCodec.MAPPER;

    private final Path file;
    private final int initialBoundaryX;
    private Persisted state;

    public FileRepartitionStateStore(Path directory, int initialBoundaryX) {
        this.file = directory.resolve("runtime-repartition-state.json");
        this.initialBoundaryX = initialBoundaryX;
        this.state = read();
    }

    @Override
    public synchronized Ownership loadOwnership() {
        return state.ownership();
    }

    @Override
    public synchronized Optional<RepartitionTransaction> loadActive() {
        return Optional.ofNullable(state.active());
    }

    @Override
    public synchronized BeginResult begin(RepartitionTransaction tx) {
        Ownership ownership = state.ownership();
        if (ownership.epoch() != tx.currentEpoch() || ownership.boundaryX() != tx.oldBoundaryX()) {
            return BeginResult.STALE_EPOCH;
        }
        if (state.active() != null) {
            return state.active().id().equals(tx.id()) ? BeginResult.IDEMPOTENT : BeginResult.CONFLICT;
        }
        state = new Persisted(ownership, tx);
        persist();
        return BeginResult.ACQUIRED;
    }

    @Override
    public synchronized boolean save(RepartitionTransaction tx) {
        if (state.active() == null || !state.active().id().equals(tx.id())) return false;
        state = new Persisted(state.ownership(), tx);
        persist();
        return true;
    }

    @Override
    public synchronized CommitResult commit(UUID transactionId, long expectedEpoch,
                                            long newEpoch, int newBoundaryX) {
        Ownership own = state.ownership();
        if (own.epoch() == newEpoch && own.boundaryX() == newBoundaryX
            && own.committedTransactionId().equals(transactionId.toString())) {
            return CommitResult.IDEMPOTENT;
        }
        if (own.epoch() != expectedEpoch) return CommitResult.STALE_EPOCH;
        if (state.active() == null || !state.active().id().equals(transactionId)) {
            return CommitResult.CONFLICT;
        }
        if (newEpoch != expectedEpoch + 1) return CommitResult.CONFLICT;
        state = new Persisted(new Ownership(newEpoch, newBoundaryX, transactionId.toString()), state.active());
        persist();
        return CommitResult.COMMITTED;
    }

    @Override
    public synchronized boolean clear(UUID transactionId) {
        if (state.active() == null) return true;
        if (!state.active().id().equals(transactionId)) return false;
        state = new Persisted(state.ownership(), null);
        persist();
        return true;
    }

    private Persisted read() {
        try {
            Files.createDirectories(file.getParent());
            if (!Files.exists(file)) {
                return new Persisted(new Ownership(0, initialBoundaryX, ""), null);
            }
            Persisted loaded = MAPPER.readValue(Files.readString(file), Persisted.class);
            if (loaded == null || loaded.ownership() == null) {
                throw new IllegalStateException("Runtime repartition state is missing ownership");
            }
            return loaded;
        } catch (IOException e) {
            throw new IllegalStateException("Could not read " + file, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Could not persist runtime repartition state to " + file, e);
        }
    }
}
