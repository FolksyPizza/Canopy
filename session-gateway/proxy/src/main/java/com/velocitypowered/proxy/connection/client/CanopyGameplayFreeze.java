/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.MinecraftConnectionAssociation;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.ClientSettingsPacket;
import com.velocitypowered.proxy.protocol.packet.KeepAlivePacket;
import com.velocitypowered.proxy.protocol.packet.ServerboundPlayerLoadedPacket;
import com.velocitypowered.proxy.protocol.packet.chat.ChatAcknowledgementPacket;
import com.velocitypowered.proxy.protocol.packet.chat.keyed.KeyedPlayerChatPacket;
import com.velocitypowered.proxy.protocol.packet.chat.legacy.LegacyChatPacket;
import com.velocitypowered.proxy.protocol.packet.chat.session.SessionPlayerChatPacket;
import com.velocitypowered.proxy.protocol.packet.config.FinishedUpdatePacket;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Session-scoped input gate for an operator-triggered maintenance cutover.
 *
 * <p>This is a bounded gameplay pause, not a world replication mechanism. The gate is before packet
 * decoding/dispatch so both Velocity packet objects and unimplemented (raw) packets receive the
 * same policy. The UUID entry survives backend attachment changes and is removed on disconnect.
 */
public final class CanopyGameplayFreeze {

  private static final org.apache.logging.log4j.Logger LOGGER =
      org.apache.logging.log4j.LogManager.getLogger(CanopyGameplayFreeze.class);
  private static final int SB_TELEPORT_ACK = 0x00;
  private static final int SB_CHAT_ACK = 0x05;
  private static final int SB_CHAT_MESSAGE = 0x08;
  private static final int SB_KEEP_ALIVE = 0x1b;
  private static final int SB_MOVE_POS = 0x1d;
  private static final int SB_MOVE_POS_ROT = 0x1e;
  private static final int SB_MOVE_ROT = 0x1f;
  private static final int SB_MOVE_STATUS_ONLY = 0x20;
  private static final int SB_VEHICLE_MOVE = 0x21;
  private static final int SB_PLAYER_LOADED = 0x2b;
  private static final int CB_PLAYER_POSITION = 0x46;
  private static final long POSITION_RELATIVE_FLAGS = 0x1fL;
  private static final long SUPPORTED_RELATIVE_FLAGS = 0x1ffL;
  private static final int MAX_TELEPORT_IDS = 64;
  private static final int SYNTHETIC_TELEPORT_START = 2_000_000_000;
  private static final long SYNTHETIC_ACK_TTL_NANOS = 30_000_000_000L;
  private static final long GENUINE_ACK_TTL_NANOS = 60_000_000_000L;
  private static final long CORRECTION_INTERVAL_NANOS = 500_000_000L;
  private static final Map<UUID, Session> SESSIONS = new ConcurrentHashMap<>();

  private CanopyGameplayFreeze() { }

  /**
   * Called for every client packet in PLAY before typed handlers or raw fallbacks run.
   * Returns true when a packet was consumed or dropped.
   */
  public static boolean beforeDispatch(ConnectedPlayer player, Object message) {
    Session session = sessionFor(player);
    if (session == null || SESSIONS.get(player.getUniqueId()) != session) {
      // A duplicate login may have replaced this connection while an old packet was queued.
      return true;
    }
    if (message instanceof ByteBuf raw) {
      boolean consumed = serverboundRaw(session, raw);
      traceFrozenPacket(session, raw, consumed);
      return consumed;
    }
    boolean consumed = session.frozen && !allowedTyped(message);
    traceFrozenPacket(session, message, consumed);
    return consumed;
  }

  private static void traceFrozenPacket(Session session, Object message, boolean consumed) {
    if (!Boolean.getBoolean("canopy.freeze.debug")) return;
    ConnectedPlayer player = session.player;
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    String packet = message.getClass().getSimpleName();
    if (message instanceof ByteBuf raw && profile != null) {
      try {
        int canonical = profile.serverbound(ProtocolUtils.readVarInt(raw.duplicate()));
        if (!session.frozen && !isMovement(canonical)) return;
        packet = "raw:" + Integer.toHexString(canonical);
      } catch (RuntimeException malformed) {
        if (!session.frozen) return;
        packet = "raw:unknown";
      }
    } else if (!session.frozen) {
      return;
    }
    LOGGER.info("Canopy freeze gate protocol={} packet={} disposition={}",
        player.getProtocolVersion().getProtocol(), packet, consumed ? "drop" : "allow");
  }

  /** Tracks authoritative backend teleports and the anchor used for visible freeze correction. */
  public static void observeClientbound(ConnectedPlayer player, ByteBuf message) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    if (profile == null) {
      return;
    }
    Session session = sessionFor(player);
    if (session == null || SESSIONS.get(player.getUniqueId()) != session) {
      return;
    }
    synchronized (session) {
      PositionCorrection correction = readPositionCorrection(profile, session.anchor, message);
      if (correction == null) {
        return;
      }
      session.anchor = correction.anchor();
      session.genuineTeleports.put(correction.teleportId(), System.nanoTime() + GENUINE_ACK_TTL_NANOS);
      trim(session.genuineTeleports);
      if (Boolean.getBoolean("canopy.freeze.debug")) {
        LOGGER.info("Canopy freeze anchor protocol={} event=position-correction parsed=true anchor=ready teleport=tracked",
            player.getProtocolVersion().getProtocol());
      }
      if (session.frozen) {
        // The backend handler will forward this exact ByteBuf after observation. Resolve the
        // relative position in place and clear only position/rotation flags so the client applies
        // one absolute correction with the backend's original teleport ID. Sending another
        // correction here would overtake the queued backend packet and double-apply its offset.
        rewriteFrozenCorrection(message, correction);
      }
    }
  }

  /** Parses the raw clientbound teleport before any handover or serving filter can consume it. */
  static @Nullable PositionCorrection readPositionCorrection(CanopyProtocolProfile profile,
      @Nullable Anchor previous, ByteBuf message) {
    int start = message.readerIndex();
    try {
      int packetId = ProtocolUtils.readVarInt(message);
      if (profile.clientbound(packetId) != CB_PLAYER_POSITION) {
        return null;
      }
      int teleportId = ProtocolUtils.readVarInt(message);
      int xIndex = message.readerIndex();
      double x = message.readDouble();
      int yIndex = message.readerIndex();
      double y = message.readDouble();
      int zIndex = message.readerIndex();
      double z = message.readDouble();
      message.readDouble(); // delta X
      message.readDouble(); // delta Y
      message.readDouble(); // delta Z
      int yawIndex = message.readerIndex();
      float yaw = message.readFloat();
      int pitchIndex = message.readerIndex();
      float pitch = message.readFloat();
      int relativesIndex = message.readerIndex();
      long relatives = message.readUnsignedInt();
      if (message.isReadable() || (relatives & ~SUPPORTED_RELATIVE_FLAGS) != 0
          || (previous == null && (relatives & POSITION_RELATIVE_FLAGS) != 0)) {
        return null;
      }
      if ((relatives & 1L) != 0) x += previous.x();
      if ((relatives & 2L) != 0) y += previous.y();
      if ((relatives & 4L) != 0) z += previous.z();
      if ((relatives & 8L) != 0) yaw += previous.yaw();
      if ((relatives & 16L) != 0) pitch += previous.pitch();
      Anchor next = anchor(x, y, z, yaw, pitch);
      return next == null ? null : new PositionCorrection(teleportId, next, xIndex, yIndex, zIndex,
          yawIndex, pitchIndex, relativesIndex, relatives);
    } catch (RuntimeException malformed) {
      return null;
    } finally {
      message.readerIndex(start);
    }
  }

  /** Resolves the position in the original packet, preserving its genuine teleport ACK identity. */
  private static void rewriteFrozenCorrection(ByteBuf message, PositionCorrection correction) {
    message.setDouble(correction.xIndex(), correction.anchor().x());
    message.setDouble(correction.yIndex(), correction.anchor().y());
    message.setDouble(correction.zIndex(), correction.anchor().z());
    message.setFloat(correction.yawIndex(), correction.anchor().yaw());
    message.setFloat(correction.pitchIndex(), correction.anchor().pitch());
    message.setInt(correction.relativesIndex(), (int) (correction.relativeFlags() & ~POSITION_RELATIVE_FLAGS));
  }

  /**
   * Freeze or release a collection of current players. Freeze validates the whole group before
   * mutation so an unknown protocol or missing position anchor cannot leave only part of a group
   * held. Runtime failures during application roll newly frozen sessions back.
   */
  public static CompletableFuture<String> setFrozen(Collection<? extends Player> players, boolean frozen) {
    if (players.isEmpty()) {
      return CompletableFuture.completedFuture("No connected players matched.");
    }
    List<Mutation> mutations = new ArrayList<>();
    for (Player apiPlayer : players) {
      if (!(apiPlayer instanceof ConnectedPlayer player)) {
        return CompletableFuture.completedFuture("Failed closed: player is not a Canopy session.");
      }
      Session session = SESSIONS.get(player.getUniqueId());
      if (session != null && !owns(session, player)) {
        return CompletableFuture.completedFuture("Failed closed: session owner changed for "
            + player.getUsername() + ".");
      }
      if (session == null) {
        if (frozen) {
          return CompletableFuture.completedFuture("Failed closed: no session/position state for "
              + player.getUsername() + ".");
        }
        continue;
      }
      synchronized (session) {
        String failure = validate(player, session, frozen);
        if (failure != null) {
          return CompletableFuture.completedFuture("Failed closed for " + player.getUsername() + ": " + failure);
        }
        mutations.add(new Mutation(session, session.frozen));
      }
    }
    if (mutations.isEmpty()) {
      return CompletableFuture.completedFuture("No tracked Canopy sessions matched.");
    }

    List<CompletableFuture<Void>> applied = new ArrayList<>();
    for (Mutation mutation : mutations) {
      applied.add(onEventLoop(mutation.session, frozen));
    }
    CompletableFuture<String> result = new CompletableFuture<>();
    CompletableFuture.allOf(applied.toArray(CompletableFuture[]::new)).whenComplete((ignored, failure) -> {
      if (failure == null) {
        result.complete((frozen ? "Frozen " : "Released ") + mutations.size() + " player session(s).");
        return;
      }
      List<CompletableFuture<Void>> rollback = new ArrayList<>();
      for (Mutation mutation : mutations) {
        if (frozen && !mutation.wasFrozen) {
          rollback.add(onEventLoop(mutation.session, false));
        } else if (!frozen && mutation.wasFrozen) {
          rollback.add(refreezeOnEventLoop(mutation.session));
        }
      }
      CompletableFuture.allOf(rollback.toArray(CompletableFuture[]::new)).whenComplete((v, rollbackFailure) -> {
        if (!frozen) {
          result.complete("Release incomplete; previously frozen sessions were "
              + (rollbackFailure == null ? "re-frozen" : "kept fail-closed where connected") + ": "
              + rootMessage(failure));
          return;
        }
        result.complete("Freeze failed closed; newly changed sessions were "
            + (rollbackFailure == null ? "released" : "not all released") + ": " + rootMessage(failure));
      });
    });
    return result;
  }

  public static String status(Collection<? extends Player> players) {
    if (players.isEmpty()) {
      return "No connected players.";
    }
    StringBuilder out = new StringBuilder();
    for (Player player : players) {
      if (out.length() != 0) out.append(" | ");
      int protocol = player.getProtocolVersion().getProtocol();
      if (CanopyProtocolProfile.find(protocol) == null) {
        out.append(player.getUsername()).append("=unsupported(protocol=").append(protocol).append(')');
        continue;
      }
      Session session = SESSIONS.get(player.getUniqueId());
      if (session != null && session.player != player) {
        session = null;
      }
      out.append(player.getUsername()).append('=').append(session != null && session.frozen ? "frozen" : "released");
      if (session == null || session.anchor == null) out.append("(no-anchor)");
      else out.append("(anchor-ready)");
    }
    return out.toString();
  }

  /** Clears all retained per-connection anchors and acknowledgements after a client disconnects. */
  static void forget(ConnectedPlayer player) {
    clientConnectionClosed(player.getConnection(), player);
  }

  /** Removes state only if the closing connection still owns the UUID's current session. */
  public static void clientConnectionClosed(MinecraftConnection connection,
      @Nullable MinecraftConnectionAssociation association) {
    if (association instanceof ConnectedPlayer player) {
      SESSIONS.computeIfPresent(player.getUniqueId(), (ignored, session) -> {
        if (session.player != player || session.connection != connection) {
          return session;
        }
        clear(session);
        return null;
      });
    }
  }

  private static Session sessionFor(ConnectedPlayer player) {
    MinecraftConnection connection = player.getConnection();
    UUID playerId = player.getUniqueId();
    if (connection.server != null) {
      Player registeredPlayer = connection.server.getPlayer(playerId).orElse(null);
      if (registeredPlayer != null && registeredPlayer != player) {
        return null;
      }
    }
    return SESSIONS.compute(playerId, (ignored, current) -> {
      if (current == null) {
        return new Session(player, connection);
      }
      if (owns(current, player)) {
        return current;
      }
      clear(current);
      return new Session(player, connection);
    });
  }

  private static boolean owns(Session session, ConnectedPlayer player) {
    return session.player == player && session.connection == player.getConnection();
  }

  private static void clear(Session session) {
    if (session != null) {
      synchronized (session) {
        session.genuineTeleports.clear();
        session.syntheticTeleports.clear();
        session.anchor = null;
        session.frozen = false;
      }
    }
  }

  static boolean hasSession(UUID playerId) {
    return SESSIONS.containsKey(playerId);
  }

  private static String validate(ConnectedPlayer player, Session session, boolean frozen) {
    if (SESSIONS.get(player.getUniqueId()) != session || !owns(session, player)) {
      return "client session owner changed";
    }
    MinecraftConnection connection = player.getConnection();
    if (!connection.getChannel().isActive() || connection.getState() != StateRegistry.PLAY
        || !(connection.getActiveSessionHandler() instanceof ClientPlaySessionHandler)) {
      return "client is not in PLAY; retry after backend transfer readiness";
    }
    if (CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol()) == null) {
      return "no explicit protocol profile";
    }
    if (frozen && session.anchor == null) {
      return "no validated position anchor";
    }
    return null;
  }

  private static CompletableFuture<Void> onEventLoop(Session session, boolean frozen) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    ConnectedPlayer player = session.player;
    MinecraftConnection connection = player.getConnection();
    try {
      connection.eventLoop().execute(() -> {
        try {
          synchronized (session) {
            String failure = validate(player, session, frozen);
            if (failure != null) throw new IllegalStateException(failure);
            if (session.frozen != frozen) {
              session.frozen = frozen;
              if (frozen && !sendCorrection(session, true)) {
                session.frozen = false;
                throw new IllegalStateException("client correction could not be sent");
              }
            }
          }
          result.complete(null);
        } catch (Throwable failure) {
          result.completeExceptionally(failure);
        }
      });
    } catch (RuntimeException rejected) {
      result.completeExceptionally(rejected);
    }
    return result;
  }

  /** Re-arms a partially released group before returning a failed release result. */
  private static CompletableFuture<Void> refreezeOnEventLoop(Session session) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    ConnectedPlayer player = session.player;
    MinecraftConnection connection = player.getConnection();
    try {
      connection.eventLoop().execute(() -> {
        try {
          synchronized (session) {
            // Close the volatile gate first. If the client is temporarily outside PLAY, input will
            // still be blocked when it returns to PLAY, even if a correction cannot be sent now.
            session.frozen = true;
            String failure = validate(player, session, true);
            if (failure != null) throw new IllegalStateException(failure);
            if (!sendCorrection(session, true)) {
              throw new IllegalStateException("client correction could not be sent while re-freezing");
            }
          }
          result.complete(null);
        } catch (Throwable failure) {
          result.completeExceptionally(failure);
        }
      });
    } catch (RuntimeException rejected) {
      session.frozen = true;
      result.completeExceptionally(rejected);
    }
    return result;
  }

  private static boolean serverboundRaw(Session session, ByteBuf message) {
    int start = message.readerIndex();
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(session.player.getProtocolVersion().getProtocol());
    if (profile == null) {
      return session.frozen;
    }
    try {
      int packetId = ProtocolUtils.readVarInt(message);
      int canonical = profile.serverbound(packetId);
      if (isMovement(canonical)) {
        synchronized (session) {
          if (!session.frozen) {
          observeMovement(session, canonical, message);
            return false;
          }
          sendCorrection(session, false);
          return true;
        }
      }
      if (canonical == SB_TELEPORT_ACK) {
        int teleportId = ProtocolUtils.readVarInt(message);
        synchronized (session) {
          expire(session.syntheticTeleports);
          if (session.syntheticTeleports.remove(teleportId) != null || syntheticId(session, teleportId)) {
            traceTeleportAck(session, "synthetic", true);
            return true;
          }
          expire(session.genuineTeleports);
          boolean genuine = session.genuineTeleports.remove(teleportId) != null;
          boolean dropped = session.frozen && !genuine;
          traceTeleportAck(session, genuine ? "genuine" : "unknown", dropped);
          return dropped;
        }
      }
      if (!session.frozen) {
        return false;
      }
      return switch (rawAction(canonical)) {
        case CHAT -> !rawChatIsPlainChat(message);
        case TRANSPORT_ACK -> false;
        case TELEPORT_ACK, MOVEMENT, DROP -> true;
      };
    } catch (RuntimeException malformedOrUnknown) {
      return session.frozen;
    } finally {
      message.readerIndex(start);
    }
  }

  static boolean allowedTyped(Object message) {
    if (message instanceof KeepAlivePacket || message instanceof ClientSettingsPacket
        || message instanceof ChatAcknowledgementPacket
        || message instanceof ServerboundPlayerLoadedPacket
        // A normal backend transfer temporarily keeps the client connection in PLAY while it
        // waits for this configuration-completion acknowledgement. It is protocol control, not
        // gameplay input; dropping it strands the frozen session between backends.
        || message instanceof FinishedUpdatePacket) {
      return true;
    }
    if (message instanceof SessionPlayerChatPacket chat) {
      return plainChat(chat.getMessage());
    }
    if (message instanceof KeyedPlayerChatPacket chat) {
      return plainChat(chat.getMessage());
    }
    if (message instanceof LegacyChatPacket chat) {
      return plainChat(chat.getMessage());
    }
    return false;
  }

  static boolean plainChat(String message) {
    return message != null && !message.startsWith("/");
  }

  static boolean rawChatIsPlainChat(ByteBuf message) {
    try {
      return plainChat(ProtocolUtils.readString(message, 256));
    } catch (RuntimeException malformed) {
      return false;
    }
  }

  private static void observeMovement(Session session, int canonical, ByteBuf message) {
    Anchor moved = readMovement(session.anchor, canonical, message);
    if (moved != null) {
      session.anchor = moved;
      if (Boolean.getBoolean("canopy.freeze.debug")) {
        LOGGER.info("Canopy freeze anchor protocol={} event=movement updated=true",
            session.player.getProtocolVersion().getProtocol());
      }
    }
  }

  static @Nullable Anchor readMovement(@Nullable Anchor prior, int canonical, ByteBuf message) {
    try {
      double x = prior == null ? Double.NaN : prior.x();
      double y = prior == null ? Double.NaN : prior.y();
      double z = prior == null ? Double.NaN : prior.z();
      float yaw = prior == null ? 0 : prior.yaw();
      float pitch = prior == null ? 0 : prior.pitch();
      switch (canonical) {
        case SB_MOVE_POS -> {
          x = message.readDouble(); y = message.readDouble(); z = message.readDouble();
          message.readUnsignedByte();
        }
        case SB_MOVE_POS_ROT -> {
          x = message.readDouble(); y = message.readDouble(); z = message.readDouble();
          yaw = message.readFloat(); pitch = message.readFloat(); message.readUnsignedByte();
        }
        case SB_MOVE_ROT -> {
          yaw = message.readFloat(); pitch = message.readFloat(); message.readUnsignedByte();
        }
        case SB_MOVE_STATUS_ONLY -> message.readUnsignedByte();
        case SB_VEHICLE_MOVE -> {
          x = message.readDouble(); y = message.readDouble(); z = message.readDouble();
          yaw = message.readFloat(); pitch = message.readFloat();
        }
        default -> { return null; }
      }
      return anchor(x, y, z, yaw, pitch);
    } catch (RuntimeException malformed) {
      return null;
    }
  }

  private static boolean isMovement(int canonical) {
    return canonical == SB_MOVE_POS || canonical == SB_MOVE_POS_ROT || canonical == SB_MOVE_ROT
        || canonical == SB_MOVE_STATUS_ONLY || canonical == SB_VEHICLE_MOVE;
  }

  private static void traceTeleportAck(Session session, String classification, boolean dropped) {
    if (!Boolean.getBoolean("canopy.freeze.debug")) return;
    LOGGER.info("Canopy freeze teleport-ack protocol={} classification={} disposition={}",
        session.player.getProtocolVersion().getProtocol(), classification, dropped ? "drop" : "allow");
  }

  static RawAction rawAction(int canonical) {
    if (canonical == SB_CHAT_MESSAGE) return RawAction.CHAT;
    if (canonical == SB_TELEPORT_ACK) return RawAction.TELEPORT_ACK;
    if (isMovement(canonical)) return RawAction.MOVEMENT;
    if (canonical == SB_CHAT_ACK || canonical == SB_KEEP_ALIVE || canonical == SB_PLAYER_LOADED) {
      return RawAction.TRANSPORT_ACK;
    }
    return RawAction.DROP;
  }

  private static @Nullable Anchor anchor(double x, double y, double z, float yaw, float pitch) {
    if (!Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(z)
        || !Float.isFinite(yaw) || !Float.isFinite(pitch)) {
      return null;
    }
    return new Anchor(x, y, z, yaw, pitch);
  }

  private static boolean sendCorrection(Session session, boolean force) {
    long now = System.nanoTime();
    if (!force && now - session.lastCorrectionNanos < CORRECTION_INTERVAL_NANOS) return true;
    Anchor anchor = session.anchor;
    if (anchor == null) return false;
    ConnectedPlayer player = session.player;
    MinecraftConnection connection = player.getConnection();
    if (!connection.getChannel().isActive() || connection.getState() != StateRegistry.PLAY) return false;
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    if (profile == null) return false;
    expire(session.syntheticTeleports);
    // The per-session high-water mark below keeps late acks recognizable after this bounded map
    // expires or is compacted, so a silent client cannot exhaust correction capacity.
    if (session.syntheticTeleports.size() >= MAX_TELEPORT_IDS) session.syntheticTeleports.clear();
    int teleportId;
    try {
      teleportId = nextTeleportId(session);
    } catch (IllegalStateException exhausted) {
      return false;
    }
    ByteBuf correction = connection.getChannel().alloc().buffer();
    writeCorrection(correction, profile, teleportId, anchor.x(), anchor.y(), anchor.z(), anchor.yaw(), anchor.pitch());
    session.syntheticTeleports.put(teleportId, System.nanoTime() + SYNTHETIC_ACK_TTL_NANOS);
    connection.write(correction);
    session.lastCorrectionNanos = now;
    return true;
  }

  private static int nextTeleportId(Session session) {
    if (session.nextSyntheticTeleportId < SYNTHETIC_TELEPORT_START
        || session.nextSyntheticTeleportId == Integer.MAX_VALUE) {
      throw new IllegalStateException("Synthetic teleport ID space is exhausted");
    }
    int candidate = session.nextSyntheticTeleportId++;
    session.latestSyntheticTeleportId = candidate;
    return candidate;
  }

  static void writeCorrection(ByteBuf out, CanopyProtocolProfile profile, int teleportId,
      double x, double y, double z, float yaw, float pitch) {
    ProtocolUtils.writeVarInt(out, profile.clientboundWire(CB_PLAYER_POSITION));
    ProtocolUtils.writeVarInt(out, teleportId);
    out.writeDouble(x).writeDouble(y).writeDouble(z);
    out.writeDouble(0).writeDouble(0).writeDouble(0);
    out.writeFloat(yaw).writeFloat(pitch).writeInt(0);
  }

  private static boolean syntheticId(Session session, int id) {
    return id >= SYNTHETIC_TELEPORT_START && id <= session.latestSyntheticTeleportId;
  }

  private static void expire(Map<Integer, Long> teleports) {
    long now = System.nanoTime();
    teleports.entrySet().removeIf(entry -> entry.getValue() < now);
  }

  private static void trim(LinkedHashMap<Integer, Long> teleports) {
    expire(teleports);
    while (teleports.size() > MAX_TELEPORT_IDS) {
      teleports.remove(teleports.keySet().iterator().next());
    }
  }

  private static String rootMessage(Throwable failure) {
    Throwable cause = failure;
    while (cause.getCause() != null) cause = cause.getCause();
    return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
  }

  record Anchor(double x, double y, double z, float yaw, float pitch) { }
  record PositionCorrection(int teleportId, Anchor anchor, int xIndex, int yIndex, int zIndex,
      int yawIndex, int pitchIndex, int relativesIndex, long relativeFlags) { }
  private record Mutation(Session session, boolean wasFrozen) { }
  enum RawAction { TELEPORT_ACK, CHAT, MOVEMENT, TRANSPORT_ACK, DROP }

  private static final class Session {
    private final ConnectedPlayer player;
    private final MinecraftConnection connection;
    private volatile @Nullable Anchor anchor;
    private volatile boolean frozen;
    private long lastCorrectionNanos;
    private final LinkedHashMap<Integer, Long> genuineTeleports = new LinkedHashMap<>();
    private final LinkedHashMap<Integer, Long> syntheticTeleports = new LinkedHashMap<>();
    private int nextSyntheticTeleportId = SYNTHETIC_TELEPORT_START;
    private int latestSyntheticTeleportId = SYNTHETIC_TELEPORT_START - 1;

    private Session(ConnectedPlayer player, MinecraftConnection connection) {
      this.player = player;
      this.connection = connection;
    }
  }
}
