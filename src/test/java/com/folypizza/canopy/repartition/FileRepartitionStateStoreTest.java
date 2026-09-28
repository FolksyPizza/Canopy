package com.folypizza.canopy.repartition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FileRepartitionStateStoreTest {
    @TempDir Path dir;

    private RepartitionTransaction tx(UUID id, long epoch) {
        return RepartitionTransaction.requested(id, 2, 1, epoch, 0, 32, -32, 32, false);
    }

    @Test
    void duplicatePrepareAndCommitAreIdempotent() {
        FileRepartitionStateStore store = new FileRepartitionStateStore(dir, 0);
        UUID id = UUID.randomUUID();
        RepartitionTransaction t = tx(id, 0);
        assertEquals(RepartitionStateStore.BeginResult.ACQUIRED, store.begin(t));
        assertEquals(RepartitionStateStore.BeginResult.IDEMPOTENT, store.begin(t));
        assertEquals(RepartitionStateStore.CommitResult.COMMITTED, store.commit(id, 0, 1, 32));
        assertEquals(RepartitionStateStore.CommitResult.IDEMPOTENT, store.commit(id, 0, 1, 32));
        assertEquals(1, store.loadOwnership().epoch());
        assertEquals(32, store.loadOwnership().boundaryX());
    }

    @Test
    void competingTransactionsCannotBothAcquireLocalAuthority() {
        FileRepartitionStateStore store = new FileRepartitionStateStore(dir, 0);
        RepartitionTransaction a = tx(UUID.randomUUID(), 0);
        RepartitionTransaction b = tx(UUID.randomUUID(), 0);
        assertEquals(RepartitionStateStore.BeginResult.ACQUIRED, store.begin(a));
        assertEquals(RepartitionStateStore.BeginResult.CONFLICT, store.begin(b));
    }

    @Test
    void staleEpochCannotRegressOwnership() {
        FileRepartitionStateStore store = new FileRepartitionStateStore(dir, 0);
        RepartitionTransaction first = tx(UUID.randomUUID(), 0);
        assertEquals(RepartitionStateStore.BeginResult.ACQUIRED, store.begin(first));
        assertEquals(RepartitionStateStore.CommitResult.COMMITTED, store.commit(first.id(), 0, 1, 32));
        store.clear(first.id());

        RepartitionTransaction stale = tx(UUID.randomUUID(), 0);
        assertEquals(RepartitionStateStore.BeginResult.STALE_EPOCH, store.begin(stale));
        assertEquals(1, store.loadOwnership().epoch());
    }

    @Test
    void restartRecoversDurableIntermediateStateAndCommittedOwnership() {
        UUID id = UUID.randomUUID();
        FileRepartitionStateStore first = new FileRepartitionStateStore(dir, 0);
        RepartitionTransaction t = tx(id, 0).transition(RepartitionPhase.PREPARING);
        first.begin(tx(id, 0));
        assertTrue(first.save(t));

        FileRepartitionStateStore restarted = new FileRepartitionStateStore(dir, 0);
        assertEquals(id, restarted.loadActive().orElseThrow().id());
        assertEquals(RepartitionPhase.PREPARING, restarted.loadActive().orElseThrow().phase());
        assertEquals(RepartitionStateStore.CommitResult.COMMITTED, restarted.commit(id, 0, 1, 32));

        FileRepartitionStateStore restartedAgain = new FileRepartitionStateStore(dir, 0);
        assertEquals(1, restartedAgain.loadOwnership().epoch());
        assertEquals(32, restartedAgain.loadOwnership().boundaryX());
    }
}
