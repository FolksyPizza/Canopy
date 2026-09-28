package com.folypizza.canopy.repartition;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeBoundaryStateTest {
    @Test
    void sourceFencingFailsClosedBeforeCommit() {
        UUID id = UUID.randomUUID();
        RepartitionTransaction tx = RepartitionTransaction.requested(id, 2, 1, 0, 0, 32, -32, 32, false);
        RuntimeBoundaryState state = new RuntimeBoundaryState(0, 0, 0, false);
        assertTrue(state.beginQuiesce(tx));
        assertTrue(state.isAuthoritative(16, 0));
        assertTrue(state.fence(id));
        assertFalse(state.isAuthoritative(16, 0));
        assertFalse(state.abort(id), "a fenced source cannot roll itself back");
        assertTrue(state.commit(id, 1, 32));
        assertEquals(1, state.epoch());
        assertEquals(32, state.boundaryX());
    }

    @Test
    void playerRaceIntoFrozenRangeIsDetectable() {
        RepartitionTransaction tx = RepartitionTransaction.requested(
            UUID.randomUUID(), 2, 1, 0, 0, 32, -32, 32, false);
        RuntimeBoundaryState state = new RuntimeBoundaryState(0, 0, 0, false);
        assertTrue(state.beginQuiesce(tx));
        assertTrue(state.isFrozen(15.5, 0));
        assertFalse(state.isFrozen(40, 0));
    }
}
