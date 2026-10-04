package com.folksypizza.canopy.session;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.util.UUID;

class SessionSnapshotTest {
    @Test void globalBedReferencePreservesNegativeChunksForcedPolicyAndView() throws Exception {
        SpawnReference bed = new SpawnReference("world_nether", -17, 64, 33, 73, -12, false);
        SessionSnapshot decoded = SessionSnapshot.decode(new SessionSnapshot("world", "east", new byte[]{4}, bed).encode());
        assertEquals(bed, decoded.spawn()); assertEquals(-2, decoded.spawn().x() >> 4);
        assertEquals("world", decoded.world()); assertEquals("east", decoded.shard());
    }
    @Test void versionOneSnapshotRemainsReadableWithoutInventingABed() throws Exception {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(1); out.writeUTF("world"); out.writeUTF("west"); out.writeInt(1); out.writeByte(4);
        assertNull(SessionSnapshot.decode(bytes.toByteArray()).spawn());
    }
    @Test void oldSessionRowsUpgradeWithoutResettingOwnership() throws Exception {
        PlayerSession state = new PlayerSession(UUID.randomUUID(), UUID.randomUUID(), "gateway",
            PlayerSession.Phase.ACTIVE, "east", null, null, 4, 8, 7, new byte[]{1});
        byte[] current = SessionCodec.encode(state);
        byte[] old = java.util.Arrays.copyOf(current, current.length - 15);
        java.nio.ByteBuffer.wrap(old).putInt(1);
        PlayerSession decoded = SessionCodec.decode(old);
        assertEquals("east", decoded.owner()); assertEquals(4, decoded.epoch());
        assertEquals(PlayerSession.Life.ALIVE, decoded.life()); assertEquals(0, decoded.lifeRevision());
    }
    @Test void corruptLifeOrSpawnDataFailsInsteadOfReturningAnEmptyPlayer() throws Exception {
        byte[] snapshot = new SessionSnapshot("world", "west", new byte[]{1},
            new SpawnReference("world", 1, 2, 3, 0, 0, true)).encode();
        assertThrows(IOException.class, () -> SessionSnapshot.decode(java.util.Arrays.copyOf(snapshot, snapshot.length - 1)));
        assertThrows(IOException.class, () -> SessionSnapshot.decode(java.util.Arrays.copyOf(snapshot, snapshot.length + 1)));
    }
}
