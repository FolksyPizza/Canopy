package com.folypizza.canopy.repartition;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hot-path ownership view used by seam listeners.
 *
 * During final commit the losing source is fenced before the durable ownership CAS. That
 * ordering can create a short availability gap, but never a period in which a stale source
 * remains authoritative after ownership may have committed elsewhere.
 */
public final class RuntimeBoundaryState {
    public record Snapshot(
        long epoch,
        int boundaryX,
        int buffer,
        boolean ownsWest,
        UUID quiescedTransaction,
        int freezeMinX,
        int freezeMaxX,
        int freezeMinZ,
        int freezeMaxZ,
        boolean fenced
    ) {
        boolean hasFreeze() { return quiescedTransaction != null; }
    }

    private final AtomicReference<Snapshot> ref;

    public RuntimeBoundaryState(long epoch, int boundaryX, int buffer, boolean ownsWest) {
        ref = new AtomicReference<>(new Snapshot(epoch, boundaryX, Math.max(0, buffer), ownsWest,
            null, 0, 0, 0, 0, false));
    }

    public Snapshot snapshot() { return ref.get(); }
    public long epoch() { return ref.get().epoch(); }
    public int boundaryX() { return ref.get().boundaryX(); }
    public int buffer() { return ref.get().buffer(); }
    public boolean ownsWest() { return ref.get().ownsWest(); }

    public boolean owns(double x) {
        Snapshot s = ref.get();
        return s.ownsWest() ? x < s.boundaryX() - s.buffer() : x >= s.boundaryX() + s.buffer();
    }

    public boolean isFrozen(double x, double z) {
        Snapshot s = ref.get();
        return s.hasFreeze() && x >= s.freezeMinX() && x < s.freezeMaxX()
            && z >= s.freezeMinZ() && z < s.freezeMaxZ();
    }

    /**
     * Whether this shard may mutate authoritative state at this coordinate.
     */
    public boolean isAuthoritative(double x, double z) {
        Snapshot s = ref.get();
        if (!owns(x)) return false;
        return !(s.fenced() && isFrozen(x, z));
    }

    public synchronized boolean beginQuiesce(RepartitionTransaction tx) {
        Snapshot s = ref.get();
        if (s.epoch() != tx.currentEpoch() || s.boundaryX() != tx.oldBoundaryX()) return false;
        if (s.quiescedTransaction() != null && !s.quiescedTransaction().equals(tx.id())) return false;
        ref.set(new Snapshot(s.epoch(), s.boundaryX(), s.buffer(), s.ownsWest(), tx.id(),
            tx.minX(), tx.maxX(), tx.minZ(), tx.maxZ(), false));
        return true;
    }

    /** Fence the old owner immediately before attempting the durable ownership commit. */
    public synchronized boolean fence(UUID txId) {
        Snapshot s = ref.get();
        if (s.quiescedTransaction() == null || !s.quiescedTransaction().equals(txId)) return false;
        ref.set(new Snapshot(s.epoch(), s.boundaryX(), s.buffer(), s.ownsWest(), s.quiescedTransaction(),
            s.freezeMinX(), s.freezeMaxX(), s.freezeMinZ(), s.freezeMaxZ(), true));
        return true;
    }

    public synchronized boolean commit(UUID txId, long newEpoch, int newBoundaryX) {
        Snapshot s = ref.get();
        if (s.epoch() == newEpoch && s.boundaryX() == newBoundaryX) return true;
        if (newEpoch != s.epoch() + 1) return false;
        if (s.quiescedTransaction() == null || !s.quiescedTransaction().equals(txId)) return false;
        ref.set(new Snapshot(newEpoch, newBoundaryX, s.buffer(), s.ownsWest(),
            null, 0, 0, 0, 0, false));
        return true;
    }

    public synchronized boolean abort(UUID txId) {
        Snapshot s = ref.get();
        if (s.fenced()) return false;
        if (s.quiescedTransaction() == null) return true;
        if (!s.quiescedTransaction().equals(txId)) return false;
        ref.set(new Snapshot(s.epoch(), s.boundaryX(), s.buffer(), s.ownsWest(),
            null, 0, 0, 0, 0, false));
        return true;
    }
}
