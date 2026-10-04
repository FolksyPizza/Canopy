package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiPredicate;

/** Client state withheld while an unready destination restores the authoritative player. */
public final class CanopyArrivalView implements AutoCloseable {
  private static final int RESPAWN = 0x50;
  private static final int FORGET_CHUNK = 0x25;
  private static final int DEATH = 0x42;
  private static final int EXPERIENCE = 0x65;
  private static final int HEALTH = 0x66;
  private static final int SLOT = 0x67;
  private final Map<Integer, ByteBuf> latest = new LinkedHashMap<>();
  private boolean repair;
  private boolean repairing;

  public void armRepair() { repair = true; }

  /** One explicitly marked, same-world native repair may preserve the existing client world. */
  public boolean consumeRepair(CanopyProtocolProfile profile, ByteBuf packet,
      BiPredicate<Integer, String> sameWorld) {
    int start = packet.readerIndex();
    try {
      int id = profile.clientbound(ProtocolUtils.readVarInt(packet));
      if (repairing && id == FORGET_CHUNK) return true;
      if (!repair || id != RESPAWN) return false;
      repair = false;
      int type = ProtocolUtils.readVarInt(packet);
      boolean compatible = sameWorld.test(type, ProtocolUtils.readString(packet));
      repairing = compatible;
      return compatible;
    } catch (RuntimeException malformed) {
      repair = false;
      repairing = false;
      return false;
    } finally { packet.readerIndex(start); }
  }

  /** Only the latest vitals are presented; a stale dead destination must not open the death screen. */
  public boolean hold(CanopyProtocolProfile profile, ByteBuf packet) {
    int start = packet.readerIndex();
    try {
      int id = profile.clientbound(ProtocolUtils.readVarInt(packet));
      if (id == DEATH) return true; // an unready attachment is not the gameplay owner
      if (id != HEALTH && id != EXPERIENCE && id != SLOT) return false;
      ByteBuf previous = latest.put(id, packet.copy(start, packet.writerIndex() - start));
      if (previous != null) previous.release();
      return true;
    } catch (RuntimeException malformed) { return false; }
    finally { packet.readerIndex(start); }
  }

  /** Transfers retained packet ownership to the caller. */
  public java.util.List<ByteBuf> drain() {
    var packets = new java.util.ArrayList<>(latest.values());
    latest.clear();
    repair = false;
    repairing = false;
    return packets;
  }

  @Override public void close() {
    latest.values().forEach(ByteBuf::release);
    latest.clear();
    repair = false;
    repairing = false;
  }
}
