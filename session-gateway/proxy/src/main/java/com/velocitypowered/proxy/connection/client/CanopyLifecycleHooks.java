/*
 * Copyright (C) 2026 Canopy Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */
package com.velocitypowered.proxy.connection.client;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** The plugin registers before login enters play, so an immediate respawn cannot bypass lifecycle routing. */
public final class CanopyLifecycleHooks {
  private static final ConcurrentHashMap<UUID, Runnable> respawns = new ConcurrentHashMap<>();
  private static final CopyOnWriteArrayList<AuditRegistration> audits = new CopyOnWriteArrayList<>();
  private static volatile boolean packetTrace;
  private static final java.util.concurrent.atomic.AtomicBoolean auditFailure = new java.util.concurrent.atomic.AtomicBoolean();
  private CanopyLifecycleHooks() {}
  public static synchronized void registerAudit(java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> sink,
      boolean trace) {
    java.util.Objects.requireNonNull(sink);
    audits.removeIf(existing -> existing.sink() == sink);
    audits.add(new AuditRegistration(sink, trace));
    packetTrace = audits.stream().anyMatch(AuditRegistration::trace);
    auditFailure.set(false);
  }
  public static synchronized void unregisterAudit(java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> sink) {
    audits.removeIf(existing -> existing.sink() == sink);
    packetTrace = audits.stream().anyMatch(AuditRegistration::trace);
  }
  public static void audit(UUID player, String event, Object... fields) {
    if (audits.isEmpty()) return;
    var data = new java.util.LinkedHashMap<String, Object>();
    data.put("event", event);
    for (int i = 0; i + 1 < fields.length; i += 2) data.put((String) fields[i], fields[i + 1]);
    for (AuditRegistration registration : audits) {
      try { registration.sink().accept(player, new java.util.LinkedHashMap<>(data)); }
      catch (RuntimeException failed) {
        if (auditFailure.compareAndSet(false, true)) org.apache.logging.log4j.LogManager.getLogger(CanopyLifecycleHooks.class)
            .warn("Canopy native audit sink failed; some incident records are missing");
      }
    }
  }
  public static void trace(UUID player, String direction, io.netty.buffer.ByteBuf packet) {
    if (!packetTrace || audits.isEmpty()) return;
    int at = packet.readerIndex(); int id = -1;
    try { id = com.velocitypowered.proxy.protocol.ProtocolUtils.readVarInt(packet); }
    catch (RuntimeException malformed) { }
    finally { packet.readerIndex(at); }
    audit(player, "packet.trace", "direction", direction, "packetId", id, "packetBytes", packet.readableBytes());
  }
  public static void trace(UUID player, String direction, Object packet) {
    if (packetTrace && !audits.isEmpty()) audit(player, "packet.decoded_trace", "direction", direction, "reason", packet.getClass().getSimpleName());
  }
  public static void register(UUID player, Runnable handler) { respawns.put(player, handler); }
  public static void unregister(UUID player, Runnable handler) { respawns.remove(player, handler); }
  static boolean requestRespawn(UUID player) {
    Runnable handler = respawns.get(player);
    if (handler == null) return false;
    handler.run(); return true;
  }

  private record AuditRegistration(
      java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> sink, boolean trace) { }
}
