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

package com.velocitypowered.proxy.connection.backend;

import com.velocitypowered.api.event.player.ServerPostConnectEvent;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.MinecraftSessionHandler;
import com.velocitypowered.proxy.connection.client.ClientPlaySessionHandler;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.protocol.MinecraftPacket;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.DisconnectPacket;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.protocol.packet.KeepAlivePacket;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;

/**
 * The backend side of a Canopy shadow session: a destination shard's session for a player who is still served by
 * another shard. Nothing reaches the client except the destination's view of the terrain it owns, which the
 * gateway's client view turns into deltas. The gateway answers the shadow's keepalives itself.
 */
public final class CanopyShadowSessionHandler implements MinecraftSessionHandler {

  public static final String PROMOTE_CHANNEL = "canopy:promote";

  private final VelocityServerConnection serverConn;
  private final ClientPlaySessionHandler playHandler;

  public CanopyShadowSessionHandler(VelocityServerConnection serverConn, ClientPlaySessionHandler playHandler) {
    this.serverConn = serverConn;
    this.playHandler = playHandler;
  }

  @Override
  public boolean handle(KeepAlivePacket packet) {
    serverConn.ensureConnected().write(packet);
    return true;
  }

  @Override
  public boolean handle(DisconnectPacket packet) {
    playHandler.getCanopyShadow().shadowLost(serverConn);
    serverConn.disconnect();
    return true;
  }

  @Override
  public boolean handle(PluginMessagePacket packet) {
    return true;
  }

  @Override
  public void handleGeneric(MinecraftPacket packet) {
    // Decoded packets (chat, boss bars, tab list, ...) belong to the ghost; the client never sees them.
  }

  @Override
  public void handleUnknown(ByteBuf buf) {
    MinecraftConnection client = serverConn.getPlayer().getConnection();
    List<ByteBuf> out = playHandler.getCanopyChunkView().onShadow(buf, client.getChannel().alloc());
    handlePreparedUnknown(out);
  }

  public void handlePreparedUnknown(List<ByteBuf> out) {
    MinecraftConnection client = serverConn.getPlayer().getConnection();
    for (ByteBuf packet : out) {
      client.delayedWrite(packet);
    }
    if (!out.isEmpty()) {
      client.flush();
    }
  }

  @Override
  public void disconnected() {
    playHandler.getCanopyShadow().shadowLost(serverConn);
  }

  /**
   * Makes a shadow session the player's serving session: the current backend is dropped, the shadow's join is
   * applied as a respawn-free switch, and the destination is told to reveal the player.
   */
  public static void promote(VelocityServer server, VelocityServerConnection shadow, JoinGamePacket joinGame,
                             ClientPlaySessionHandler playHandler) {
    ConnectedPlayer player = shadow.getPlayer();
    VelocityServerConnection existing = player.getConnectedServer();
    RegisteredServer previous = existing == null ? null : existing.getServer();
    if (existing != null) {
      player.setConnectedServer(null);
      existing.disconnect();
    }
    MinecraftConnection smc = shadow.ensureConnected();
    shadow.setCanopyShadow(false);
    playHandler.handleBackendJoinGame(joinGame, shadow);
    smc.setActiveSessionHandler(StateRegistry.PLAY, CanopyServingSessionHandler.wrap(
        new BackendPlaySessionHandler(server, shadow), smc, playHandler));
    player.setConnectedServer(shadow);
    smc.write(new PluginMessagePacket(PROMOTE_CHANNEL, Unpooled.EMPTY_BUFFER));
    server.getEventManager().fireAndForget(new ServerPostConnectEvent(player, previous));
  }
}
