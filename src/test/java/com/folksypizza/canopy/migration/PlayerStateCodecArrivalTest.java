package com.folksypizza.canopy.migration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.bukkit.entity.Player;
import org.junit.jupiter.api.Test;

class PlayerStateCodecArrivalTest {
    private Player untouchedPlayer() {
        return (Player) Proxy.newProxyInstance(Player.class.getClassLoader(), new Class<?>[]{Player.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "getName" -> "CanopyProbe";
                case "isDead" -> true;
                default -> throw new AssertionError("Player accessed before decoded-state validation: " + method.getName());
            });
    }

    private byte[] payload(double health, int inventorySize) throws Exception {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(4);
        out.writeDouble(73.25); out.writeDouble(64.5); out.writeDouble(-19.75);
        out.writeFloat(81); out.writeFloat(-13);
        for (int i = 0; i < 5; i++) out.writeDouble(0);
        out.writeLong(0); out.writeInt(0); // movement time and rocket
        out.writeUTF("SURVIVAL");
        out.writeBoolean(false); out.writeBoolean(false); out.writeBoolean(false);
        out.writeDouble(health); out.writeInt(20); out.writeFloat(5);
        out.writeFloat(.5f); out.writeInt(42); out.writeInt(4);
        out.writeInt(inventorySize);
        out.writeInt(0); out.writeInt(0); out.writeInt(0); out.writeInt(0); // armor, offhand, ender, effects
        return bytes.toByteArray();
    }

    @Test void deadOrInvalidIncomingHealthCannotForceRespawnOrRestoreProgress() throws Exception {
        for (double health : new double[]{0, -1, Double.NaN, Double.POSITIVE_INFINITY}) {
            AtomicInteger repairs = new AtomicInteger();
            assertNull(PlayerStateCodec.apply(untouchedPlayer(), payload(health, 0), null,
                arrival -> repairs.incrementAndGet()));
            assertEquals(0, repairs.get());
        }
    }

    @Test void truncatedOrTrailingDataCannotTriggerANativeRespawn() throws Exception {
        byte[] valid = payload(7, 0);
        for (byte[] invalid : new byte[][]{java.util.Arrays.copyOf(valid, valid.length - 1),
                java.util.Arrays.copyOf(valid, valid.length + 1)}) {
            AtomicInteger repairs = new AtomicInteger();
            assertNull(PlayerStateCodec.apply(untouchedPlayer(), invalid, null, arrival -> repairs.incrementAndGet()));
            assertEquals(0, repairs.get());
        }
    }

    @Test void oversizedInventoryIsRefusedBeforeAnyRecoveryMutation() throws Exception {
        AtomicInteger repairs = new AtomicInteger();
        assertNull(PlayerStateCodec.apply(untouchedPlayer(), payload(7, Integer.MAX_VALUE), null,
            arrival -> repairs.incrementAndGet()));
        assertEquals(0, repairs.get());
    }

    @Test void recoveryReceivesExactSnapshotDestinationAndView() throws Exception {
        AtomicInteger repairs = new AtomicInteger();
        assertNull(PlayerStateCodec.apply(untouchedPlayer(), payload(7, 0), null, arrival -> {
            assertEquals(73.25, arrival.getX()); assertEquals(64.5, arrival.getY());
            assertEquals(-19.75, arrival.getZ()); assertEquals(81, arrival.getYaw());
            assertEquals(-13, arrival.getPitch()); repairs.incrementAndGet();
            throw new IllegalStateException("Stop before mutating the fake player");
        }));
        assertEquals(1, repairs.get());
    }

    @Test void invalidTotalExperienceCannotTriggerARepairInTheNewFormat() throws Exception {
        byte[] old = payload(7, 0), modern = java.util.Arrays.copyOf(old, old.length + 4);
        java.nio.ByteBuffer.wrap(modern).putInt(5).position(old.length).putInt(-1);
        AtomicInteger repairs = new AtomicInteger();
        assertNull(PlayerStateCodec.apply(untouchedPlayer(), modern, null, arrival -> repairs.incrementAndGet()));
        assertEquals(0, repairs.get());
    }

    @Test void defaultRestoreCannotApplyAliveProgressDirectlyToADeadNativePlayer() throws Exception {
        assertNull(PlayerStateCodec.apply(untouchedPlayer(), payload(7, 0), null));
    }
}
