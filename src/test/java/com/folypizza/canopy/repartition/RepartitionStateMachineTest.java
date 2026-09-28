package com.folypizza.canopy.repartition;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RepartitionStateMachineTest {
    private static RepartitionTransaction tx() {
        return RepartitionTransaction.requested(UUID.randomUUID(), 2, 1, 7, 0, 32, -32, 32, false);
    }

    @Test
    void happyPathHasOneIrreversibleCommitBoundary() {
        RepartitionTransaction t = tx()
            .transition(RepartitionPhase.PREPARING)
            .transition(RepartitionPhase.QUIESCING)
            .transition(RepartitionPhase.SNAPSHOTTING)
            .transition(RepartitionPhase.TRANSFERRING)
            .transition(RepartitionPhase.DESTINATION_READY)
            .transition(RepartitionPhase.COMMITTING);
        assertFalse(t.phase().isAbortable());
        t = t.transition(RepartitionPhase.COMMITTED)
            .transition(RepartitionPhase.CLEANING_UP)
            .transition(RepartitionPhase.COMPLETE);
        assertEquals(RepartitionPhase.COMPLETE, t.phase());
        assertFalse(t.phase().canTransitionTo(RepartitionPhase.ABORTING));
    }

    @Test
    void illegalRollbackIsRejected() {
        RepartitionTransaction t = tx().transition(RepartitionPhase.PREPARING);
        assertThrows(IllegalStateException.class, () -> t.transition(RepartitionPhase.REQUESTED));
    }

    @Test
    void invalidPartitionCoordinatesAreRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> RepartitionTransaction.requested(UUID.randomUUID(), 1, 2, 0, 0, 17, -32, 32, true));
        assertThrows(IllegalArgumentException.class,
            () -> RepartitionTransaction.requested(UUID.randomUUID(), 1, 2, 0, 0, 16, 0, 0, true));
    }
}
