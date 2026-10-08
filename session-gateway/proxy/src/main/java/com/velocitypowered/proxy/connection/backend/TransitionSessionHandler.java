/*
 * Copyright (C) 2019-2023 Velocity Contributors
 * Copyright (C) 2026 Canopy Contributors
 * Modified by Canopy contributors in 2026.
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

package com.velocitypowered.proxy.connection.backend;

import static com.velocitypowered.proxy.connection.backend.BackendConnectionPhases.IN_TRANSITION;
import static com.velocitypowered.proxy.connection.forge.legacy.LegacyForgeHandshakeBackendPhase.HELLO;

import com.velocitypowered.api.event.player.ServerConnectedEvent;
import com.velocitypowered.api.event.player.ServerPostConnectEvent;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.ConnectionTypes;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.MinecraftSessionHandler;
import com.velocitypowered.proxy.connection.client.ClientPlaySessionHandler;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.connection.util.ConnectionMessages;
import com.velocitypowered.proxy.connection.util.ConnectionRequestResults;
import com.velocitypowered.proxy.connection.util.ConnectionRequestResults.Impl;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.DisconnectPacket;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.protocol.packet.KeepAlivePacket;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A special session handler that catches "last minute" disconnects.
 */
public class TransitionSessionHandler implements MinecraftSessionHandler {

  private static final Logger logger = LogManager.getLogger(TransitionSessionHandler.class);

  private final VelocityServer server;
  private final VelocityServerConnection serverConn;
  private final CompletableFuture<Impl> resultFuture;
  private final BungeeCordMessageResponder bungeecordMessageResponder;
  private final java.util.ArrayDeque<Object> deferredChatJoin = new java.util.ArrayDeque<>();
  private int deferredChatBytes;

  private boolean chatPending() {
    return serverConn.getPlayer().getConnection().getActiveSessionHandler() instanceof ClientPlaySessionHandler play
        && play.signedChat().attemptId(serverConn) != null;
  }

  private void deferChatJoin(Object packet) {
    int size;
    try { size = com.velocitypowered.proxy.connection.client.CanopySignedChatHandoff.packetSize(packet,
        com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.CLIENTBOUND,
        serverConn.getPlayer().getProtocolVersion(), 8388608); }
    catch (RuntimeException encodingFailure) {
      ((ClientPlaySessionHandler) serverConn.getPlayer().getConnection().getActiveSessionHandler())
          .signedChat().abort(encodingFailure);
      releaseDeferred(); return;
    }
    if (deferredChatJoin.size() >= 256 || deferredChatBytes + size > 8388608) {
      ((ClientPlaySessionHandler) serverConn.getPlayer().getConnection().getActiveSessionHandler())
          .signedChat().abort(new IllegalStateException("Destination chat join output exceeded bound"));
      releaseDeferred();
      return;
    }
    deferredChatBytes += size;
    deferredChatJoin.add(io.netty.util.ReferenceCountUtil.retain(packet));
  }

  @Override public void handleUnknown(io.netty.buffer.ByteBuf packet) {
    if (!chatPending()) return;
    com.velocitypowered.proxy.connection.client.CanopyProtocolProfile profile =
        com.velocitypowered.proxy.connection.client.CanopyProtocolProfile.find(serverConn.getPlayer().getProtocolVersion().getProtocol());
    if (profile != null && profile.clientbound(com.velocitypowered.proxy.protocol.ProtocolUtils.readVarInt(packet.duplicate())) == 0x3f
        && !((ClientPlaySessionHandler) serverConn.getPlayer().getConnection().getActiveSessionHandler())
            .signedChat().restored()) return;
    deferChatJoin(packet);
  }

  @Override public void handleGeneric(com.velocitypowered.proxy.protocol.MinecraftPacket packet) {
    if (chatPending()) deferChatJoin(packet);
  }

  private void releaseDeferred() {
    deferredChatJoin.forEach(io.netty.util.ReferenceCountUtil::release);
    deferredChatJoin.clear(); deferredChatBytes = 0;
  }


  /**
   * Creates the new transition handler.
   *
   * @param server       the Velocity server instance
   * @param serverConn   the server connection
   * @param resultFuture the result future
   */
  TransitionSessionHandler(VelocityServer server,
      VelocityServerConnection serverConn,
      CompletableFuture<Impl> resultFuture) {
    this.server = server;
    this.serverConn = serverConn;
    this.resultFuture = resultFuture;
    this.bungeecordMessageResponder = new BungeeCordMessageResponder(server,
        serverConn.getPlayer());
  }

  @Override
  public boolean beforeHandle() {
    if (!serverConn.isActive()) {
      // Obsolete connection
      serverConn.disconnect();
      return true;
    }
    return false;
  }

  @Override
  public boolean handle(KeepAlivePacket packet) {
    serverConn.ensureConnected().write(packet);
    return true;
  }

  @Override
  public boolean handle(JoinGamePacket packet) {
    final MinecraftConnection smc = serverConn.ensureConnected();
    if (serverConn.isCanopyShadow()) {
      // A shadow session joins but does not replace the current backend: park it until it is promoted.
      if (serverConn.getPlayer().getConnection().getActiveSessionHandler()
          instanceof ClientPlaySessionHandler playHandler) {
        playHandler.getCanopyShadow().attached(serverConn, packet);
        smc.setActiveSessionHandler(StateRegistry.PLAY,
            CanopyServingSessionHandler.wrap(new CanopyShadowSessionHandler(serverConn, playHandler), smc, playHandler));
        resultFuture.complete(ConnectionRequestResults.successful(serverConn.getServer()));
      } else {
        serverConn.disconnect();
      }
      return true;
    }
    // Validate the no-respawn contract while the source connection is still available.
    if (serverConn.getCanopyMode()
        == com.velocitypowered.proxy.connection.client.CanopySwitchPolicy.Mode.SEAMLESS
        && serverConn.getPlayer().getConnection().getActiveSessionHandler()
            instanceof ClientPlaySessionHandler play
        && !play.canopySameWorld(packet)) {
      play.signedChat().abort(new IllegalStateException("Seamless destination world mismatch"));
      play.getCanopyHandover().cancel();
      resultFuture.complete(ConnectionRequestResults.forDisconnect(
          DisconnectPacket.create(net.kyori.adventure.text.Component.text(
              "SEAMLESS cannot switch between different client worlds"),
              serverConn.getPlayer().getProtocolVersion(), StateRegistry.PLAY), serverConn.getServer()));
      serverConn.disconnect();
      return true;
    }
    if (serverConn.getPlayer().getConnection().getActiveSessionHandler() instanceof ClientPlaySessionHandler play
        && serverConn.getCanopyMode() != com.velocitypowered.proxy.connection.client.CanopySwitchPolicy.Mode.NORMAL
        && play.signedChat().required()) {
      var restoreAttempt = play.signedChat().attemptId(serverConn);
      play.signedChat().restore(serverConn).whenCompleteAsync((unused, failure) -> {
        if (failure != null) resultFuture.completeExceptionally(failure);
        else if (!play.signedChat().ownsAttempt(restoreAttempt, serverConn)
            || !play.signedChat().canCommit(serverConn)) {
          var expired = new IllegalStateException("Chat handoff no longer owns source/destination");
          play.signedChat().abortAttempt(restoreAttempt, serverConn, expired);
          resultFuture.completeExceptionally(expired);
        } else finishJoin(packet, smc);
      }, smc.eventLoop());
    } else finishJoin(packet, smc);
    return true;
  }

  private void finishJoin(JoinGamePacket packet, MinecraftConnection smc) {
    if (!serverConn.isActive()) {
      resultFuture.completeExceptionally(new IllegalStateException("Destination closed before source detach"));
      return;
    }
    final RegisteredServer previousServer = serverConn.getPreviousServer().orElse(null);
    final ConnectedPlayer player = serverConn.getPlayer();
    final VelocityServerConnection existingConnection = player.getConnectedServer();

    final ClientPlaySessionHandler nativePlay = player.getConnection().getActiveSessionHandler()
        instanceof ClientPlaySessionHandler play && play.signedChat().required()
        && serverConn.getCanopyMode() != com.velocitypowered.proxy.connection.client.CanopySwitchPolicy.Mode.NORMAL
        ? play : null;
    final java.util.UUID nativeAttempt = nativePlay == null ? null : nativePlay.signedChat().attemptId(serverConn);
    final java.util.concurrent.atomic.AtomicBoolean finishStarted = new java.util.concurrent.atomic.AtomicBoolean();
    final java.util.concurrent.atomic.AtomicBoolean finishCommitted = new java.util.concurrent.atomic.AtomicBoolean();
    if (nativePlay == null) detachSource(player, existingConnection, packet);

    // Retain the native source throughout asynchronous connection listeners. The final checked
    // detach, join and chat commit then run together on the shared player/backend event loop.
    smc.setAutoReading(false);
    var connectedEvent = server.getEventManager()
        .fire(new ServerConnectedEvent(player, serverConn.getServer(), previousServer));
    Runnable finish = () -> {
          // Make sure we can still transition (player might have disconnected here).
          if (!serverConn.isActive()) {
            // Connection is obsolete.
            serverConn.disconnect();
            return;
          }

          if (nativePlay != null) {
            finishStarted.set(true);
            detachSource(player, existingConnection, packet);
          }

          // Change the client to use the ClientPlaySessionHandler if required.
          ClientPlaySessionHandler playHandler;
          if (player.getConnection()
              .getActiveSessionHandler() instanceof ClientPlaySessionHandler sessionHandler) {
            playHandler = sessionHandler;
          } else {
            playHandler = new ClientPlaySessionHandler(server, player);
            player.getConnection().setActiveSessionHandler(StateRegistry.PLAY, playHandler);
          }
          assert playHandler != null;
          playHandler.handleBackendJoinGame(packet, serverConn);

          // Set the new play session handler for the server. We will have nothing more to do
          // with this connection once this task finishes up.
          smc.setActiveSessionHandler(StateRegistry.PLAY,
              CanopyServingSessionHandler.wrap(new BackendPlaySessionHandler(server, serverConn), smc, playHandler));

          // Deliver destination startup output only after native signed state restoration.
          var backendHandler = smc.getActiveSessionHandler();
          while (!deferredChatJoin.isEmpty()) {
            Object queued = deferredChatJoin.remove();
            try {
              if (queued instanceof io.netty.buffer.ByteBuf raw) backendHandler.handleUnknown(raw);
              else if (queued instanceof com.velocitypowered.proxy.protocol.MinecraftPacket decoded
                  && !decoded.handle(backendHandler)) backendHandler.handleGeneric(decoded);
            } finally { io.netty.util.ReferenceCountUtil.release(queued); }
          }
          deferredChatBytes = 0;

          // Now set the connected server.
          serverConn.getPlayer().setConnectedServer(serverConn);

          // Clean up disabling auto-read while the connected event was being processed.
          // Do this after setting the connection, so no incoming packets are processed before
          // the API knows which server the player is connected to.
          smc.setAutoReading(true);
          if (playHandler.signedChat().required() && serverConn.getCanopyMode()
              != com.velocitypowered.proxy.connection.client.CanopySwitchPolicy.Mode.NORMAL) {
            playHandler.signedChat().commit(playHandler);
            finishCommitted.set(true);
          }

          // Send client settings. In 1.20.2+ this is done in the config state.
          if (smc.getProtocolVersion().lessThan(ProtocolVersion.MINECRAFT_1_20_2)
              && player.getClientSettingsPacket() != null) {
            serverConn.ensureConnected().write(player.getClientSettingsPacket());
          }

          // We're done! :)
          server.getEventManager().fireAndForget(new ServerPostConnectEvent(player,
              previousServer));
          resultFuture.complete(ConnectionRequestResults.successful(serverConn.getServer()));
        };
    CompletableFuture<Void> completed = nativePlay == null
        ? connectedEvent.thenRunAsync(finish, smc.eventLoop())
        : nativePlay.signedChat().finishAfter(nativeAttempt, connectedEvent, serverConn, smc.eventLoop(), finish);
    completed.exceptionallyAsync(exc -> {
      releaseDeferred();
      if (nativePlay != null && !nativePlay.signedChat().shouldRemediateFailure(nativeAttempt,
          serverConn, existingConnection, finishStarted.get(), finishCommitted.get())) {
        resultFuture.completeExceptionally(exc);
        return null;
      }
      if (nativePlay != null && existingConnection != null && existingConnection.isActive()
          && player.getConnectedServer() == existingConnection) {
        nativePlay.signedChat().abortAttempt(nativeAttempt, serverConn, exc);
      } else {
        logger.error("Unable to switch to new server {} for {}",
            serverConn.getServerInfo().getName(), player.getUsername(), exc);
        player.disconnect(ConnectionMessages.INTERNAL_SERVER_CONNECTION_ERROR);
      }
      resultFuture.completeExceptionally(exc);
      return null;
    }, smc.eventLoop());
  }

  private void detachSource(ConnectedPlayer player, VelocityServerConnection existing,
      JoinGamePacket packet) {
    if (existing != null) {
      player.setConnectedServer(null);
      existing.disconnect();
      player.sendKeepAlive();
    }
    player.clearPlayerListHeaderAndFooter();
    packet.setOnlineMode(player.isOnlineMode());
  }

  @Override
  public boolean handle(DisconnectPacket packet) {
    if (chatPending()) ((ClientPlaySessionHandler) serverConn.getPlayer().getConnection().getActiveSessionHandler())
        .signedChat().abort(new IllegalStateException("Destination disconnected during chat restore"));
    releaseDeferred();
    final MinecraftConnection connection = serverConn.ensureConnected();
    serverConn.disconnect();

    // If we were in the middle of the Forge handshake, it is not safe to proceed. We must kick
    // the client.
    if (connection.getType() == ConnectionTypes.LEGACY_FORGE
        && !serverConn.getPhase().consideredComplete()) {
      resultFuture.complete(ConnectionRequestResults.forUnsafeDisconnect(packet,
          serverConn.getServer()));
    } else {
      resultFuture.complete(ConnectionRequestResults.forDisconnect(packet, serverConn.getServer()));
    }

    return true;
  }

  @Override
  public boolean handle(PluginMessagePacket packet) {
    if (serverConn.getPlayer().getConnection().getActiveSessionHandler() instanceof ClientPlaySessionHandler play
        && play.signedChat().receive(serverConn, packet)) return true;
    if (chatPending()) { deferChatJoin(packet); return true; }
    if (bungeecordMessageResponder.process(packet)) {
      return true;
    }

    // We always need to handle plugin messages, for Forge compatibility.
    if (serverConn.getPhase().handle(serverConn, serverConn.getPlayer(), packet)) {
      // Handled, but check the server connection phase.
      if (serverConn.getPhase() == HELLO) {
        VelocityServerConnection existingConnection = serverConn.getPlayer().getConnectedServer();
        if (existingConnection != null && existingConnection.getPhase() != IN_TRANSITION) {
          // Indicate that this connection is "in transition"
          existingConnection.setConnectionPhase(IN_TRANSITION);

          // Tell the player that we're leaving and we just aren't coming back.
          existingConnection.getPhase().onDepartForNewServer(existingConnection,
              serverConn.getPlayer());
        }
      }
      return true;
    }

    serverConn.getPlayer().getConnection().write(packet.retain());
    return true;
  }

  @Override
  public void disconnected() {
    if (chatPending()) ((ClientPlaySessionHandler) serverConn.getPlayer().getConnection().getActiveSessionHandler())
        .signedChat().abort(new IllegalStateException("Destination closed during chat restore"));
    releaseDeferred();
    resultFuture.complete(ConnectionRequestResults.forDisconnect(
        ConnectionMessages.INTERNAL_SERVER_CONNECTION_ERROR, serverConn.getServer()));
  }
}
