package com.folypizza.canopy.repartition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic lightweight model test for retries, failures and restarts. */
class RepartitionModelTest {
    @TempDir Path dir;

    @Test
    void epochNeverDecreasesAcrossRetrySequences() {
        Random random = new Random(0xCA70F1L);
        long observedEpoch = 0;
        int boundary = 0;

        for (int round = 0; round < 20; round++) {
            FileRepartitionStateStore store = new FileRepartitionStateStore(dir, boundary);
            RepartitionStateStore.Ownership ownership = store.loadOwnership();
            assertTrue(ownership.epoch() >= observedEpoch);
            observedEpoch = ownership.epoch();
            boundary = ownership.boundaryX();

            UUID id = UUID.randomUUID();
            int nextBoundary = boundary + 16;
            RepartitionTransaction tx = RepartitionTransaction.requested(
                id, 2, 1, ownership.epoch(), boundary, nextBoundary, -16, 16, false);

            assertEquals(RepartitionStateStore.BeginResult.ACQUIRED, store.begin(tx));
            if (random.nextBoolean()) {
                assertEquals(RepartitionStateStore.BeginResult.IDEMPOTENT, store.begin(tx));
            }

            if (random.nextInt(4) == 0) {
                assertTrue(store.clear(id));
                continue;
            }

            RepartitionStateStore.CommitResult committed =
                store.commit(id, ownership.epoch(), ownership.epoch() + 1, nextBoundary);
            assertTrue(committed == RepartitionStateStore.CommitResult.COMMITTED
                || committed == RepartitionStateStore.CommitResult.IDEMPOTENT);
            if (random.nextBoolean()) {
                assertEquals(RepartitionStateStore.CommitResult.IDEMPOTENT,
                    store.commit(id, ownership.epoch(), ownership.epoch() + 1, nextBoundary));
            }
            store.clear(id);

            FileRepartitionStateStore restarted = new FileRepartitionStateStore(dir, 0);
            assertEquals(ownership.epoch() + 1, restarted.loadOwnership().epoch());
            assertTrue(restarted.loadOwnership().epoch() >= observedEpoch);
            observedEpoch = restarted.loadOwnership().epoch();
            boundary = restarted.loadOwnership().boundaryX();
        }
    }
}
