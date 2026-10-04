package com.velocitypowered.proxy.connection.client;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** The plugin registers before login enters play, so an immediate respawn cannot bypass lifecycle routing. */
public final class CanopyLifecycleHooks {
  private static final ConcurrentHashMap<UUID, Runnable> respawns = new ConcurrentHashMap<>();
  private static volatile java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> audit;
  private static volatile boolean packetTrace;
  private static final java.util.concurrent.atomic.AtomicBoolean auditFailure = new java.util.concurrent.atomic.AtomicBoolean();
  private CanopyLifecycleHooks() {}
  public static synchronized void registerAudit(java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> sink,
      boolean trace) { audit = java.util.Objects.requireNonNull(sink); packetTrace = trace; auditFailure.set(false); }
  public static synchronized void unregisterAudit(java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> sink) {
    if (audit == sink) { audit = null; packetTrace = false; }
  }
  public static void audit(UUID player, String event, Object... fields) {
    var sink = audit;
    if (sink == null) return;
    var data = new java.util.LinkedHashMap<String, Object>();
    data.put("event", event);
    for (int i = 0; i + 1 < fields.length; i += 2) data.put((String) fields[i], fields[i + 1]);
    try { sink.accept(player, data); }
    catch (RuntimeException failed) {
      if (auditFailure.compareAndSet(false, true)) org.apache.logging.log4j.LogManager.getLogger(CanopyLifecycleHooks.class)
          .warn("Canopy native audit sink failed; some incident records are missing");
    }
  }
  public static void trace(UUID player, String direction, io.netty.buffer.ByteBuf packet) {
    if (!packetTrace || audit == null) return;
    int at = packet.readerIndex(); int id = -1;
    try { id = com.velocitypowered.proxy.protocol.ProtocolUtils.readVarInt(packet); }
    catch (RuntimeException malformed) { }
    finally { packet.readerIndex(at); }
    audit(player, "packet.trace", "direction", direction, "packetId", id, "packetBytes", packet.readableBytes());
  }
  public static void trace(UUID player, String direction, Object packet) {
    if (packetTrace) audit(player, "packet.decoded_trace", "direction", direction, "reason", packet.getClass().getSimpleName());
  }
  public static void register(UUID player, Runnable handler) { respawns.put(player, handler); }
  public static void unregister(UUID player, Runnable handler) { respawns.remove(player, handler); }
  static boolean requestRespawn(UUID player) {
    Runnable handler = respawns.get(player);
    if (handler == null) return false;
    handler.run(); return true;
  }
}
