package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CanopyLifecycleHooksTest {
  @Test void departingOldConnectionCannotUnregisterAReconnectedPlayer() {
    UUID player = UUID.randomUUID();
    AtomicInteger oldCalls = new AtomicInteger(), newCalls = new AtomicInteger();
    Runnable old = oldCalls::incrementAndGet, replacement = newCalls::incrementAndGet;
    try {
      CanopyLifecycleHooks.register(player, old);
      CanopyLifecycleHooks.register(player, replacement);
      CanopyLifecycleHooks.unregister(player, old);
      assertTrue(CanopyLifecycleHooks.requestRespawn(player));
      assertEquals(0, oldCalls.get());
      assertEquals(1, newCalls.get());
    } finally { CanopyLifecycleHooks.unregister(player, replacement); }
    assertFalse(CanopyLifecycleHooks.requestRespawn(player));
  }

  @Test void everyTargetProtocolHasTheNativeRespawnRequestMapping() {
    for (int protocol = 771; protocol <= 777; protocol++) {
      var profile = CanopyProtocolProfile.find(protocol);
      assertEquals(protocol <= 774 ? 0x0b : 0x0c, profile.serverboundWire(0x0b));
    }
  }
  @Test void auditTracePreservesPacketAndOldShutdownCannotRemoveNewSink() {
    var rows = new java.util.ArrayList<java.util.Map<String, Object>>();
    java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> old = (id, fields) -> {};
    java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> replacement = (id, fields) -> rows.add(fields);
    UUID player = UUID.randomUUID();
    var packet = io.netty.buffer.Unpooled.buffer();
    packet.writeByte(0x1d); packet.writeByte(42);
    try {
      CanopyLifecycleHooks.registerAudit(old, false);
      CanopyLifecycleHooks.registerAudit(replacement, true);
      CanopyLifecycleHooks.unregisterAudit(old);
      CanopyLifecycleHooks.trace(player, "serverbound", packet);
      assertEquals(0, packet.readerIndex()); assertEquals(2, packet.readableBytes());
      assertEquals(1, rows.size());
      assertEquals(0x1d, rows.getFirst().get("packetId"));
      assertEquals(2, rows.getFirst().get("packetBytes"));
      assertFalse(rows.getFirst().containsKey("payload"));
    } finally { packet.release(); CanopyLifecycleHooks.unregisterAudit(replacement); }
    CanopyLifecycleHooks.audit(player, "test.after_close"); assertEquals(1, rows.size());
  }

  @Test void failingAuditSinkCannotBreakRespawnOrPacketHandling() {
    java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> broken = (id, fields) -> {
      throw new IllegalStateException("synthetic failure");
    };
    UUID player = UUID.randomUUID();
    AtomicInteger requests = new AtomicInteger(); Runnable respawn = requests::incrementAndGet;
    try {
      CanopyLifecycleHooks.registerAudit(broken, false);
      CanopyLifecycleHooks.register(player, respawn);
      assertDoesNotThrow(() -> CanopyLifecycleHooks.audit(player, "test.failure"));
      assertTrue(CanopyLifecycleHooks.requestRespawn(player)); assertEquals(1, requests.get());
    } finally { CanopyLifecycleHooks.unregisterAudit(broken); CanopyLifecycleHooks.unregister(player, respawn); }
  }

}
