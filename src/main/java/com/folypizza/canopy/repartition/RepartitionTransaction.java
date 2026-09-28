package com.folypizza.canopy.repartition;

import java.util.Objects;
import java.util.UUID;

/**
 * Durable transaction record. The transaction id and proposed epoch are stable across every retry.
 */
public record RepartitionTransaction(
    UUID id,
    long sourceShardId,
    long destinationShardId,
    long currentEpoch,
    long proposedEpoch,
    int oldBoundaryX,
    int newBoundaryX,
    int minZ,
    int maxZ,
    boolean sourceOwnsWest,
    RepartitionPhase phase,
    long createdAtMillis,
    long updatedAtMillis,
    int expectedChunks,
    int transferredChunks,
    long bytesTransferred,
    String failureReason,
    boolean commitOutcomeUnknown
) {
    public RepartitionTransaction {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(phase, "phase");
        failureReason = failureReason == null ? "" : failureReason;
        if (sourceShardId == destinationShardId) throw new IllegalArgumentException("source and destination must differ");
        if (proposedEpoch != currentEpoch + 1) throw new IllegalArgumentException("proposed epoch must be current+1");
        RepartitionValidation.requireChunkAligned(oldBoundaryX, "old boundary");
        RepartitionValidation.requireChunkAligned(newBoundaryX, "new boundary");
        RepartitionValidation.requireChunkAligned(minZ, "min-z");
        RepartitionValidation.requireChunkAligned(maxZ, "max-z");
        if (oldBoundaryX == newBoundaryX) throw new IllegalArgumentException("boundary move is a no-op");
        if (minZ >= maxZ) throw new IllegalArgumentException("min-z must be less than max-z");
        if (expectedChunks <= 0) throw new IllegalArgumentException("transaction must affect at least one chunk");
        if (transferredChunks < 0 || transferredChunks > expectedChunks) {
            throw new IllegalArgumentException("invalid transferred chunk count");
        }
    }

    public static RepartitionTransaction requested(UUID id, long sourceShardId, long destinationShardId,
                                                    long currentEpoch, int oldBoundaryX, int newBoundaryX,
                                                    int minZ, int maxZ, boolean sourceOwnsWest) {
        long now = System.currentTimeMillis();
        int expected = RepartitionValidation.affectedChunkCount(oldBoundaryX, newBoundaryX, minZ, maxZ);
        return new RepartitionTransaction(id, sourceShardId, destinationShardId, currentEpoch, currentEpoch + 1,
            oldBoundaryX, newBoundaryX, minZ, maxZ, sourceOwnsWest, RepartitionPhase.REQUESTED,
            now, now, expected, 0, 0, "", false);
    }

    public RepartitionTransaction transition(RepartitionPhase next) {
        phase.requireTransition(next);
        return copy(next, transferredChunks, bytesTransferred, failureReason, commitOutcomeUnknown);
    }

    public RepartitionTransaction progress(int chunks, long bytes) {
        if (chunks < transferredChunks || chunks > expectedChunks) {
            throw new IllegalArgumentException("progress cannot decrease or exceed expected chunks");
        }
        if (bytes < bytesTransferred) throw new IllegalArgumentException("byte progress cannot decrease");
        return copy(phase, chunks, bytes, failureReason, commitOutcomeUnknown);
    }

    public RepartitionTransaction failed(String reason, boolean commitUnknown) {
        return copy(RepartitionPhase.FAILED, transferredChunks, bytesTransferred, reason, commitUnknown);
    }

    private RepartitionTransaction copy(RepartitionPhase next, int chunks, long bytes,
                                        String reason, boolean commitUnknown) {
        return new RepartitionTransaction(id, sourceShardId, destinationShardId, currentEpoch, proposedEpoch,
            oldBoundaryX, newBoundaryX, minZ, maxZ, sourceOwnsWest, next, createdAtMillis,
            System.currentTimeMillis(), expectedChunks, chunks, bytes, reason, commitUnknown);
    }

    public int minX() { return Math.min(oldBoundaryX, newBoundaryX); }
    public int maxX() { return Math.max(oldBoundaryX, newBoundaryX); }

    public boolean containsBlock(int x, int z) {
        return x >= minX() && x < maxX() && z >= minZ && z < maxZ;
    }

    public boolean containsChunk(int chunkX, int chunkZ) {
        return containsBlock(chunkX << 4, chunkZ << 4);
    }
}
