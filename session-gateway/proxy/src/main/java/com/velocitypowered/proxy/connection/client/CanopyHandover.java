/*
 * Copyright (C) 2018-2025 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayDeque;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Exact shard handover: a consistent cut of the player's input between two Canopy shards.
 *
 * <p>When the source shard asks for a switch ({@code canopy:switch}), the gateway stops forwarding the client's
 * gameplay input to it and buffers that input instead, then injects a {@code canopy:cut} marker into the source's
 * inbound stream. The source reaches the marker only after every input the gateway forwarded before it, so the
 * state it snapshots there is exact. The destination applies that state server-side; while it does, the gateway
 * hides its position corrections from the client (which is already where it should be) and confirms them itself.
 * On {@code canopy:ready} the gateway replays the buffered input to the destination, which catches up to the
 * client. Nothing is extrapolated, and the client never sees a correction.
 *
 * <p>Explicit protocol profiles gate the input cut; SEAMLESS replays input without a client world reset.
 */
public final class CanopyHandover {

  private static final Logger logger = LogManager.getLogger(CanopyHandover.class);

  public static final String SWITCH_CHANNEL = "canopy:switch";
  public static final String CUT_CHANNEL = "canopy:cut";
  public static final String CANCEL_CHANNEL = "canopy:cut-cancel";
  public static final String READY_CHANNEL = "canopy:ready";
  public static final String REPAIR_CHANNEL = "canopy:respawn-repair";

  // Keep the experimental packet cut off unless an isolated test explicitly enables it.
  private static final boolean ENABLED = Boolean.getBoolean("canopy.exactHandover");

  // Serverbound, protocol 774: the input that is replayed to the destination.
  private static final int SB_ACCEPT_TELEPORTATION = 0x00;
  private static final int SB_MOVE_POS = 0x1d;
  private static final int SB_MOVE_POS_ROT = 0x1e;
  private static final int SB_MOVE_ROT = 0x1f;
  private static final int SB_MOVE_STATUS_ONLY = 0x20;
  private static final int SB_PLAYER_COMMAND = 0x29;
  private static final int SB_PLAYER_INPUT = 0x2a;
  private static final int SB_SET_CARRIED_ITEM = 0x34;
  private static final int SB_CLIENT_COMMAND = 0x0b;
  // Protocol acknowledgements that belong to whichever backend is current; never held back.
  private static final int SB_CHUNK_BATCH_RECEIVED = 0x0a;
  private static final int SB_CLIENT_TICK_END = 0x0c;
  private static final int SB_PLAYER_LOADED = 0x2b;
  private static final int SB_PONG = 0x2c;
  // Clientbound, protocol 774.
  private static final int CB_PLAYER_POSITION = 0x46;

  /** Report a slow handoff without treating a timeout as proof that player state was restored. */
  private static final long CUT_TIMEOUT_MILLIS = 10_000L;
  private static final long ARRIVAL_TIMEOUT_MILLIS = 3_000L;
  private static final int MAX_BUFFERED = 2_000;

  private enum State { IDLE, CUT, ARRIVING }

  private final ConnectedPlayer player;
  private final ArrayDeque<ByteBuf> buffered = new ArrayDeque<>();
  private State state = State.IDLE;
  private long stateSince;
  private VelocityServerConnection source;
  private boolean timeoutReported;
  private boolean bufferOverflowReported;
  private boolean arrivalRepairAllowed;
  private final CanopyArrivalView arrivalView = new CanopyArrivalView();

  private boolean currentArrival(MinecraftConnection destination) {
    var current = player.getConnectedServer();
    return state == State.ARRIVING && current != null && current != source
        && current.getConnection() == destination;
  }

  public synchronized void armRespawnRepair(MinecraftConnection destination) {
    if (currentArrival(destination) && enabled()) {
      arrivalView.armRepair();
    }
  }

  /** The live source's ordered cut permits recovery of a stale native copy on the next attachment. */
  public synchronized void prepareRespawnRepair(MinecraftConnection connection) {
    if (state == State.CUT && source != null && source.getConnection() == connection
        && enabled()) arrivalRepairAllowed = true;
  }

  /** Run before chunk-view computation: a hidden native repair must not clear cached client chunks. */
  public synchronized boolean consumeRepairReset(ByteBuf packet, MinecraftConnection destination,
      com.velocitypowered.proxy.connection.backend.CanopyEntityRewriter rewriter) {
    if (!currentArrival(destination) || !enabled()) return false;
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    boolean consumed = arrivalView.consumeRepair(profile, packet, rewriter::sameWorld);
    if (consumed && profile.clientbound(peekId(packet)) == 0x50) {
      audit("respawn.native_repair_hidden");
      // Native respawn restarts the backend's load timer. The preserved client cannot acknowledge a hidden reset.
      destination.eventLoop().execute(() -> {
        var current = player.getConnectedServer();
        if (!destination.isClosed() && current != null && current.getConnection() == destination) {
          destination.write(com.velocitypowered.proxy.protocol.packet.ServerboundPlayerLoadedPacket.INSTANCE);
        }
      });
    }
    return consumed;
  }

  CanopyHandover(ConnectedPlayer player) {
    this.player = player;
  }

  /**
   * A {@code canopy:switch} payload is the target server (modified UTF-8) followed, from shards that support it, by
   * one byte: 1 asks for an exact handover. Older shards send only the name and keep the projected handover.
   */
  public static boolean requestsExact(ByteBuf content) {
    int start = content.readerIndex();
    try {
      int nameLength = content.readUnsignedShort();
      content.skipBytes(nameLength);
      return content.isReadable() && content.readByte() == 1;
    } catch (Exception ex) {
      return false;
    } finally {
      content.readerIndex(start);
    }
  }

  /** The target server named in a {@code canopy:switch} payload, or null. */
  public static String switchTarget(ByteBuf content) {
    int start = content.readerIndex();
    try {
      int length = content.readUnsignedShort();
      return content.readCharSequence(length, java.nio.charset.StandardCharsets.UTF_8).toString();
    } catch (Exception ex) {
      return null;
    } finally {
      content.readerIndex(start);
    }
  }

  /** The optional exact-handover correlation id appended after the exact-request byte. */
  public static UUID switchTransferId(ByteBuf content) {
    int start = content.readerIndex();
    try {
      int nameLength = content.readUnsignedShort();
      content.skipBytes(nameLength);
      if (!content.isReadable() || content.readUnsignedByte() != 1 || content.readableBytes() != 16) {
        return null;
      }
      return new UUID(content.readLong(), content.readLong());
    } catch (Exception ex) {
      return null;
    } finally {
      content.readerIndex(start);
    }
  }

  /** A {@code canopy:cut-done} payload is exactly the correlation id for one armed crossing. */
  public static UUID acknowledgementTransferId(ByteBuf content) {
    int start = content.readerIndex();
    try {
      if (content.readableBytes() != 16) return null;
      return new UUID(content.readLong(), content.readLong());
    } catch (Exception ex) {
      return null;
    } finally {
      content.readerIndex(start);
    }
  }

  private boolean enabled() {
    return ENABLED && CanopySwitchPolicy.mayUseExactHandover(player.getUniqueId())
        && CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol()) != null;
  }

  /** The source asked for a switch: hold input from here and mark the cut in the source's stream. */
  public synchronized void begin(VelocityServerConnection source) {
    if (!enabled() || state != State.IDLE) {
      return;
    }
    MinecraftConnection smc = source.getConnection();
    if (smc == null || smc.isClosed()) {
      return;
    }
    this.source = source;
    this.state = State.CUT;
    this.stateSince = System.currentTimeMillis();
    audit("input.cut");
    smc.write(new PluginMessagePacket(CUT_CHANNEL, Unpooled.EMPTY_BUFFER));
  }

  /** A durable dead session has no gameplay input to cut; CSG routes its actual respawn request. */
  public synchronized void beginRespawn() {
    if (state != State.IDLE) throw new IllegalStateException("Another handoff is active");
    source = player.getConnectedServer();
    state = State.CUT; stateSince = System.currentTimeMillis();
    audit("respawn.cut");
  }

  /** Runs on the client connection loop after the owner commit; the backend performs vanilla respawn. */
  public synchronized void sendRespawnRequest() {
    if (state != State.IDLE) ready();
    var current = player.getConnectedServer();
    if (current == null || current.getConnection() == null || current.getConnection().isClosed()) return;
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    if (profile == null) throw new IllegalStateException("Unsupported managed respawn protocol");
    ByteBuf request = current.getConnection().getChannel().alloc().buffer();
    ProtocolUtils.writeVarInt(request, profile.serverboundWire(SB_CLIENT_COMMAND));
    ProtocolUtils.writeVarInt(request, 0);
    current.getConnection().write(request);
    audit("respawn.native_request");
  }

  /** The destination's join packet arrived and the client was switched to it. */
  public synchronized void arrived() {
    if (state == State.CUT) {
      state = State.ARRIVING;
      audit("handoff.arriving");
      stateSince = System.currentTimeMillis();
      if (arrivalRepairAllowed) arrivalView.armRepair();
    }
  }

  /** The destination has applied the handed-over state: replay the held input to it. */
  public synchronized void ready() {
    if (state == State.IDLE) {
      return;
    }
    for (ByteBuf packet : arrivalView.drain()) player.getConnection().delayedWrite(packet);
    player.getConnection().flush();
    flushTo(player.getConnectedServer());
    audit("input.ready");
    reset();
  }

  /** The switch fell back to a respawn (a different world): held movement no longer applies. */
  public synchronized void abandon() {
    if (state != State.IDLE) audit("handoff.abandoned");
    release();
    reset();
  }

  /** The switch failed: return the held input to the source and let it resume the player. */
  public synchronized void cancel() {
    if (state == State.IDLE) {
      return;
    }
    audit("input.cancel");
    if (source != null && source.getConnection() != null && !source.getConnection().isClosed()) {
      source.getConnection().write(new PluginMessagePacket(CANCEL_CHANNEL, Unpooled.EMPTY_BUFFER));
      flushTo(source);
    } else {
      release();
    }
    reset();
  }

  /**
   * A raw serverbound packet from the client. Returns true when it was held back or dropped, false when it should
   * be forwarded as usual.
   */
  public synchronized boolean onServerbound(ByteBuf buf) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    if (profile != null && profile.serverbound(peekId(buf)) == SB_CLIENT_COMMAND) {
      int at = buf.readerIndex();
      try {
        ProtocolUtils.readVarInt(buf);
        if (ProtocolUtils.readVarInt(buf) == 0 && !buf.isReadable()
            && CanopyLifecycleHooks.requestRespawn(player.getUniqueId())) return true;
      } catch (RuntimeException malformed) { /* the backend handles malformed client commands */ }
      finally { buf.readerIndex(at); }
    }
    if (state == State.IDLE) {
      return false;
    }
    if (expired() && !timeoutReported) {
      timeoutReported = true;
      audit("handoff.timeout");
      logger.warn("Canopy handover for {} timed out in {}; holding input until readiness or cancellation",
          player.getUsername(), state);
    }
    int id = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol()).serverbound(peekId(buf));
    switch (id) {
      case SB_MOVE_POS, SB_MOVE_POS_ROT, SB_MOVE_ROT, SB_MOVE_STATUS_ONLY,
          SB_PLAYER_COMMAND, SB_PLAYER_INPUT, SB_SET_CARRIED_ITEM -> {
        if (buffered.size() < MAX_BUFFERED) {
          buffered.add(buf.retainedDuplicate());
        } else if (!bufferOverflowReported) {
          bufferOverflowReported = true; audit("input.buffer_overflow");
        }
        return true;
      }
      case SB_CHUNK_BATCH_RECEIVED, SB_CLIENT_TICK_END, SB_PLAYER_LOADED, SB_PONG, SB_ACCEPT_TELEPORTATION -> {
        return false;
      }
      default -> {
        // Anything else would act on a shard that no longer owns the player (or on one that is not ready).
        return true;
      }
    }
  }

  /**
   * A raw clientbound packet from the destination while it applies the handed-over state. Position corrections
   * are confirmed here and never reach the client, which is already in the right place. Returns true if consumed.
   */
  public synchronized boolean onClientbound(ByteBuf buf, MinecraftConnection destination) {
    if (state != State.ARRIVING) {
      return false;
    }
    if (currentArrival(destination) && arrivalView.hold(
        CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol()), buf)) return true;
    int start = buf.readerIndex();
    try {
      if (CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol())
          .clientbound(ProtocolUtils.readVarInt(buf)) != CB_PLAYER_POSITION) {
        return false;
      }
      int teleportId = ProtocolUtils.readVarInt(buf);
      ByteBuf confirm = destination.getChannel().alloc().buffer();
      ProtocolUtils.writeVarInt(confirm, CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol())
          .serverboundWire(SB_ACCEPT_TELEPORTATION));
      ProtocolUtils.writeVarInt(confirm, teleportId);
      destination.write(confirm);
      audit("position.correction_hidden");
      return true;
    } catch (Exception ex) {
      return false;
    } finally {
      buf.readerIndex(start);
    }
  }

  public synchronized boolean active() {
    return state != State.IDLE;
  }

  private boolean expired() {
    long limit = state == State.CUT ? CUT_TIMEOUT_MILLIS : ARRIVAL_TIMEOUT_MILLIS;
    return System.currentTimeMillis() - stateSince > limit;
  }

  private void flushTo(VelocityServerConnection target) {
    MinecraftConnection smc = target == null ? null : target.getConnection();
    if (smc == null || smc.isClosed()) {
      release();
      return;
    }
    int count = buffered.size();
    // Entity ids in the held input are translated now, for the backend it is replayed to.
    var rewriter = ((ClientPlaySessionHandler) player.getConnection().getActiveSessionHandler()).getCanopyRewriter();
    boolean translate = rewriter.active(player.getProtocolVersion());
    ByteBuf packet;
    while ((packet = buffered.poll()) != null) {
      if (translate) {
        ByteBuf out = rewriter.processServerbound(packet);
        if (out != packet) {
          packet.release();
        }
        smc.delayedWrite(out);
      } else {
        smc.delayedWrite(packet);
      }
    }
    smc.flush();
    CanopyLifecycleHooks.audit(player.getUniqueId(), "input.replayed", "target", target.getServerInfo().getName(),
        "count", count, "chunks", ((ClientPlaySessionHandler) player.getConnection().getActiveSessionHandler()).getCanopyChunkView().stats());
    logger.info("Canopy handover for {}: replayed {} input packet(s) to {}; chunks so far: {}", player.getUsername(),
        count, target.getServerInfo().getName(),
        ((ClientPlaySessionHandler) player.getConnection().getActiveSessionHandler()).getCanopyChunkView().stats());
  }

  private void release() {
    ByteBuf packet;
    while ((packet = buffered.poll()) != null) {
      packet.release();
    }
  }

  private void reset() {
    arrivalView.close();
    state = State.IDLE;
    timeoutReported = false; bufferOverflowReported = false;
    arrivalRepairAllowed = false;
    source = null;
  }

  private void audit(String event) {
    CanopyLifecycleHooks.audit(player.getUniqueId(), event, "phase", state.name(), "buffered", buffered.size(),
        "protocol", player.getProtocolVersion().getProtocol(), "owner", source == null ? null : source.getServerInfo().getName(),
        "durationMs", state == State.IDLE ? 0L : System.currentTimeMillis() - stateSince);
  }

  private static int peekId(ByteBuf buf) {
    int start = buf.readerIndex();
    try {
      return ProtocolUtils.readVarInt(buf);
    } catch (Exception ex) {
      return -1;
    } finally {
      buf.readerIndex(start);
    }
  }
}
