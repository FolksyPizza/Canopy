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

import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.backend.CanopyShadowSessionHandler;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.server.VelocityRegisteredServer;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A player's Canopy shadow session: while they approach a seam, the gateway holds a second session with the
 * destination shard (the player joins it as a hidden ghost). The destination's terrain on its side of the seam reaches
 * the client as deltas before any crossing, and a crossing promotes the shadow instead of opening a new connection.
 * The source keeps the shadow alive with {@code canopy:shadow} refreshes; without one for two seconds it closes.
 */
public final class CanopyShadow {

  private static final Logger logger = LogManager.getLogger(CanopyShadow.class);
  private static final long REFRESH_TIMEOUT_MILLIS = 2_000L;

  private final VelocityServer server;
  private final ConnectedPlayer player;
  private final ClientPlaySessionHandler playHandler;

  private String target;
  private double boundaryX;
  private boolean destinationOwnsEast;
  private VelocityServerConnection conn;
  private JoinGamePacket joinGame;
  private long lastRefresh;
  private final CanopyShadowPromotionGate promotion = new CanopyShadowPromotionGate();
  private ScheduledFuture<?> ticker;

  CanopyShadow(VelocityServer server, ConnectedPlayer player, ClientPlaySessionHandler playHandler) {
    this.server = server;
    this.player = player;
    this.playHandler = playHandler;
  }

  /** The source asks for (or refreshes) a shadow with {@code target}, whose side of the seam at x = boundaryX is given. */
  public synchronized void request(String target, double boundaryX, boolean destinationOwnsEast) {
    if (!Boolean.getBoolean("canopy.shadow") || !Boolean.getBoolean("canopy.shadow.experimental")
        || CanopySwitchPolicy.requested() != CanopySwitchPolicy.Mode.SEAMLESS
        || Boolean.getBoolean("canopy.sessions") || Boolean.getBoolean("canopy.managed.sessions.enabled")) {
      return; // native ghost lifecycle is not safe for production or managed sessions yet
    }
    lastRefresh = System.currentTimeMillis();
    if (conn != null && target.equalsIgnoreCase(this.target)) {
      return;
    }
    close();
    VelocityServerConnection current = player.getConnectedServer();
    var registered = server.getServer(target).orElse(null);
    if (current == null || !(registered instanceof VelocityRegisteredServer destination)
        || current.getServerInfo().getName().equalsIgnoreCase(target)) {
      return;
    }
    this.target = target;
    this.boundaryX = boundaryX;
    this.destinationOwnsEast = destinationOwnsEast;
    VelocityServerConnection shadow = new VelocityServerConnection(destination, current.getServer(), player, server);
    shadow.setCanopyShadow(true);
    this.conn = shadow;
    CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.request", "target", target);
    logger.info("Canopy shadow for {}: joining {}", player.getUsername(), target);
    shadow.connect().whenComplete((result, error) -> {
      if (error != null || result == null || !result.isSuccessful()) {
        CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.failed", "target", target, "error", error == null ? null : error.getClass().getName());
        logger.warn("Canopy shadow for {} on {} failed{}", player.getUsername(), target,
            error == null ? "" : ": " + error.getMessage());
        shadowLost(shadow);
      }
    });
    if (ticker == null) {
      ticker = player.getConnection().eventLoop().scheduleAtFixedRate(this::tick, 250, 250, TimeUnit.MILLISECONDS);
    }
  }

  /** The shadow joined the destination: its view of the destination's side now reaches the client. */
  public synchronized void attached(VelocityServerConnection shadow, JoinGamePacket joinGame) {
    if (shadow != conn) {
      shadow.disconnect();
      return;
    }
    this.joinGame = joinGame;
    CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.attached", "target", target);
    double seam = boundaryX;
    boolean east = destinationOwnsEast;
    playHandler.getCanopyChunkView().shadowStarted(cx -> east ? cx * 16 >= seam : cx * 16 + 16 <= seam);
    logger.info("Canopy shadow for {} joined {}", player.getUsername(), target);
  }

  /** A crossing to {@code target} with an attached shadow: promote it once the source confirms its snapshot. */
  public synchronized boolean armPromotion(String target, VelocityServerConnection source, UUID transferId) {
    if (conn == null || joinGame == null || source == null || player.getConnectedServer() != source
        || target == null || !target.equalsIgnoreCase(this.target)) {
      return false;
    }
    return promotion.arm(source, target, transferId, System.nanoTime());
  }

  public synchronized boolean promotionArmed() {
    return promotion.isPending();
  }

  /** Accept one source-bound acknowledgement for the currently armed shadow transfer. */
  public synchronized boolean acknowledgeAndPromote(VelocityServerConnection source, UUID transferId) {
    long now = System.nanoTime();
    if (promotion.expired(now)) {
      CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.confirmation_timeout", "target", target);
      logger.warn("Canopy shadow for {}: no valid snapshot acknowledgement; aborting the handover", player.getUsername());
      close();
      return false;
    }
    if (source == null || player.getConnectedServer() != source || conn == null || joinGame == null
        || conn.getConnection() == null || conn.getConnection().isClosed()
        || !promotion.acknowledge(source, target, transferId, now)) {
      CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.acknowledgement_rejected", "target", target);
      return false;
    }
    if (!promotion.consumeAcknowledgement()) return false;
    promoteConfirmedShadow();
    return true;
  }

  /** The source's correlated acknowledgement arrived over the source backend connection. */
  private void promoteConfirmedShadow() {
    CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.promoted", "target", target);
    VelocityServerConnection shadow = conn;
    JoinGamePacket join = joinGame;
    reset();
    playHandler.getCanopyChunkView().shadowEnded();
    logger.info("Canopy shadow for {} promoted: {} now serves the player", player.getUsername(),
        shadow.getServerInfo().getName());
    CanopyShadowSessionHandler.promote(server, shadow, join, playHandler);
  }

  public synchronized void close() {
    if (conn != null) {
      CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.closed", "target", target);
      logger.info("Canopy shadow for {} on {} closed", player.getUsername(), target);
      conn.disconnect();
    }
    VelocityServerConnection pendingSource = reset();
    playHandler.getCanopyChunkView().shadowEnded();
    abortPendingHandover(pendingSource);
  }

  /** The shadow's connection ended on its own (the destination refused or dropped the ghost). */
  public synchronized void shadowLost(VelocityServerConnection shadow) {
    if (shadow == conn) {
      CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.lost", "target", target);
      VelocityServerConnection pendingSource = reset();
      playHandler.getCanopyChunkView().shadowEnded();
      abortPendingHandover(pendingSource);
    }
  }

  private VelocityServerConnection reset() {
    VelocityServerConnection pendingSource = promotion.isPending()
        ? (VelocityServerConnection) promotion.source() : null;
    promotion.clear();
    conn = null;
    joinGame = null;
    target = null;
    return pendingSource;
  }

  private void abortPendingHandover(VelocityServerConnection source) {
    if (source == null || !player.isActive()) return;
    try {
      player.getConnection().eventLoop().execute(() -> {
        if (!player.isActive()) return;
        if (player.getConnectedServer() == source) {
          playHandler.getCanopyHandover().cancel();
        } else {
          playHandler.getCanopyHandover().abandon();
        }
      });
    } catch (RuntimeException eventLoopClosed) {
      // The client disconnect path owns final ByteBuf cleanup; no stale shadow may be promoted here.
      CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.abort_release_unavailable", "target", target);
    }
  }

  private void tick() {
    synchronized (this) {
      if (!player.isActive()) {
        close();
        if (ticker != null) {
          ticker.cancel(false);
          ticker = null;
        }
        return;
      }
      long nowNanos = System.nanoTime();
      if (promotion.expired(nowNanos)) {
        CanopyLifecycleHooks.audit(player.getUniqueId(), "shadow.confirmation_timeout", "target", target);
        logger.warn("Canopy shadow for {}: no valid snapshot acknowledgement; aborting the handover", player.getUsername());
        close();
        return;
      }
      long now = System.currentTimeMillis();
      if (!promotion.isPending() && conn != null && now - lastRefresh > REFRESH_TIMEOUT_MILLIS) {
        close();
      }
    }
  }
}
