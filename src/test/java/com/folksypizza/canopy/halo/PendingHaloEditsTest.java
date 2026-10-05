package com.folksypizza.canopy.halo;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PendingHaloEditsTest {
    @Test
    void coalescesLatestBlockStateAndDrainsOnlyTheRequestedChunk() {
        var queue = new PendingHaloEdits(4);
        assertTrue(queue.offer(new PendingHaloEdits.Edit(1, -1, 62, -17, "minecraft:water[level=1]")));
        assertTrue(queue.offer(new PendingHaloEdits.Edit(2, -1, 62, -17, "minecraft:air")));
        assertTrue(queue.offer(new PendingHaloEdits.Edit(1, -1, 62, -17, "minecraft:water[level=1]")));
        assertTrue(queue.offer(new PendingHaloEdits.Edit(3, 0, 62, -17, "minecraft:stone")));

        assertEquals(2, queue.size());
        assertTrue(queue.hasChunk(-1, -2));
        var drained = queue.drain(-1, -2);
        assertEquals(1, drained.size());
        assertEquals(2, drained.getFirst().sequence());
        assertEquals("minecraft:air", drained.getFirst().data());
        assertEquals(1, queue.size());
        assertFalse(queue.hasChunk(-1, -2));
        assertEquals(1, queue.drain(0, -2).size());
        assertEquals(0, queue.size());
    }

    @Test
    void capacityRejectsNewBlocksButAllowsCoalescingAndRecoversAfterDrain() {
        var queue = new PendingHaloEdits(1);
        assertTrue(queue.offer(new PendingHaloEdits.Edit(1, 2, 64, 2, "minecraft:dirt")));
        assertFalse(queue.offer(new PendingHaloEdits.Edit(2, 3, 64, 2, "minecraft:stone")));
        assertTrue(queue.offer(new PendingHaloEdits.Edit(3, 2, 64, 2, "minecraft:grass_block")));
        assertEquals(1, queue.size());
        assertEquals("minecraft:grass_block", queue.drain(0, 0).getFirst().data());
        assertTrue(queue.offer(new PendingHaloEdits.Edit(4, 3, 64, 2, "minecraft:stone")));
    }

    @Test
    void capacityMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new PendingHaloEdits(0));
    }
}
