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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.client.CanopyHandover;
import com.velocitypowered.proxy.connection.client.CanopySwitchPolicy;
import com.velocitypowered.proxy.connection.client.ClientPlaySessionHandler;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import com.velocitypowered.proxy.util.VelocityChannelRegistrar;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class BackendPlaySessionHandlerBedrockHandoverTest {
  @Test
  void exactBedrockSwitchArmsOnceAndSendsOneCutMarker() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = trial(id, CanopySwitchPolicy.Platform.BEDROCK,
        "canopy.allowSeamless", "true", "canopy.experimentalBedrockSeamless", "true")) {
      Fixture fixture = fixture(id, 776);
      when(fixture.server().getServer("east")).thenReturn(Optional.of(mock(RegisteredServer.class)));
      PluginMessagePacket request = switchRequest("east", true);
      try {
        fixture.handler().handle(request);
        fixture.handler().handle(request);

        assertTrue(fixture.playHandler().getCanopyHandover().active());
        ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(fixture.sourceConnection(), times(1)).write(writes.capture());
        PluginMessagePacket marker = (PluginMessagePacket) writes.getValue();
        try {
          assertTrue(CanopyHandover.CUT_CHANNEL.equals(marker.getChannel()));
        } finally {
          ReferenceCountUtil.release(marker);
        }
      } finally {
        ReferenceCountUtil.release(request);
        fixture.playHandler().getCanopyHandover().abandon();
      }
    }
  }

  @Test
  void exactBedrockSwitchWithoutBothTrialFlagsDoesNotArmOrSendCut() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = trial(id, CanopySwitchPolicy.Platform.BEDROCK,
        "canopy.allowSeamless", "true", "canopy.experimentalBedrockSeamless", "false")) {
      Fixture fixture = fixture(id, 776);
      when(fixture.server().getServer("east")).thenReturn(Optional.of(mock(RegisteredServer.class)));
      PluginMessagePacket request = switchRequest("east", true);
      try {
        fixture.handler().handle(request);

        assertFalse(fixture.playHandler().getCanopyHandover().active());
        verify(fixture.sourceConnection(), never()).write(org.mockito.ArgumentMatchers.any());
      } finally {
        ReferenceCountUtil.release(request);
      }
    }
  }

  @Test
  void nonExactBedrockSwitchLeavesProjectedPathUnchanged() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = trial(id, CanopySwitchPolicy.Platform.BEDROCK,
        "canopy.allowSeamless", "true", "canopy.experimentalBedrockSeamless", "true")) {
      Fixture fixture = fixture(id, 776);
      PluginMessagePacket request = switchRequest("east", false);
      try {
        fixture.handler().handle(request);

        assertFalse(fixture.playHandler().getCanopyHandover().active());
        verify(fixture.server(), never()).getServer("east");
        verify(fixture.sourceConnection(), never()).write(org.mockito.ArgumentMatchers.any());
      } finally {
        ReferenceCountUtil.release(request);
      }
    }
  }

  @Test
  void exactJavaSwitchStillUsesOnlyTheExistingExactGate() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = trial(id, CanopySwitchPolicy.Platform.JAVA,
        "canopy.allowSeamless", "false", "canopy.experimentalBedrockSeamless", "false")) {
      Fixture fixture = fixture(id, 776);
      PluginMessagePacket request = switchRequest("east", true);
      try {
        fixture.handler().handle(request);

        if (Boolean.getBoolean("canopy.exactHandover")) {
          assertTrue(fixture.playHandler().getCanopyHandover().active());
          ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
          verify(fixture.sourceConnection(), times(1)).write(writes.capture());
          PluginMessagePacket marker = (PluginMessagePacket) writes.getValue();
          try {
            assertTrue(CanopyHandover.CUT_CHANNEL.equals(marker.getChannel()));
          } finally {
            ReferenceCountUtil.release(marker);
          }
        } else {
          assertFalse(fixture.playHandler().getCanopyHandover().active());
          verify(fixture.sourceConnection(), never()).write(org.mockito.ArgumentMatchers.any());
        }
      } finally {
        ReferenceCountUtil.release(request);
        fixture.playHandler().getCanopyHandover().abandon();
      }
    }
  }

  private static Fixture fixture(UUID id, int protocol) {
    VelocityServer server = mock(VelocityServer.class);
    VelocityConfiguration configuration = mock(VelocityConfiguration.class);
    VelocityChannelRegistrar registrar = mock(VelocityChannelRegistrar.class);
    when(server.getConfiguration()).thenReturn(configuration);
    when(configuration.isBungeePluginChannelEnabled()).thenReturn(false);
    when(server.getChannelRegistrar()).thenReturn(registrar);
    when(registrar.getFromId(anyString())).thenReturn(null);

    ConnectedPlayer player = mock(ConnectedPlayer.class);
    MinecraftConnection clientConnection = mock(MinecraftConnection.class);
    when(player.getUniqueId()).thenReturn(id);
    when(player.getUsername()).thenReturn("FixtureClient");
    when(player.getConnection()).thenReturn(clientConnection);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
    when(clientConnection.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
    ClientPlaySessionHandler playHandler = new ClientPlaySessionHandler(server, player);
    when(clientConnection.getActiveSessionHandler()).thenReturn(playHandler);

    VelocityServerConnection source = mock(VelocityServerConnection.class);
    MinecraftConnection sourceConnection = mock(MinecraftConnection.class);
    when(source.getPlayer()).thenReturn(player);
    when(source.getConnection()).thenReturn(sourceConnection);
    when(source.getServerInfo()).thenReturn(new ServerInfo("main",
        new InetSocketAddress("127.0.0.1", 25565)));
    when(source.getPhase()).thenReturn(BackendConnectionPhases.VANILLA);
    when(sourceConnection.isClosed()).thenReturn(false);
    when(sourceConnection.getState()).thenReturn(StateRegistry.PLAY);
    when(player.getConnectedServer()).thenReturn(source);

    return new Fixture(server, source, sourceConnection, player, playHandler,
        new BackendPlaySessionHandler(server, source));
  }

  private static PluginMessagePacket switchRequest(String target, boolean exact) {
    byte[] name = target.getBytes(StandardCharsets.UTF_8);
    ByteBuf content = Unpooled.buffer();
    content.writeShort(name.length).writeBytes(name).writeByte(exact ? 1 : 0);
    return new PluginMessagePacket(CanopyHandover.SWITCH_CHANNEL, content);
  }

  private static PropertyRestore trial(UUID player, CanopySwitchPolicy.Platform platform,
                                       String... extraProperties) {
    List<String> properties = new ArrayList<>(List.of("canopy.mode", "SEAMLESS",
        "canopy.allowSeamless", "false", "canopy.experimentalBedrockSeamless", "false"));
    properties.addAll(List.of(extraProperties));
    PropertyRestore restore = new PropertyRestore(player, properties.toArray(String[]::new));
    CanopySwitchPolicy.markPlatform(player, platform);
    CanopySwitchPolicy.seamless(player, true);
    return restore;
  }

  private record Fixture(VelocityServer server, VelocityServerConnection source,
                         MinecraftConnection sourceConnection, ConnectedPlayer player,
                         ClientPlaySessionHandler playHandler, BackendPlaySessionHandler handler) { }

  private static final class PropertyRestore implements AutoCloseable {
    private final UUID player;
    private final List<String> names = new ArrayList<>();
    private final List<String> values = new ArrayList<>();

    PropertyRestore(UUID player, String... nameValues) {
      this.player = player;
      for (int i = 0; i < nameValues.length; i += 2) {
        String name = nameValues[i];
        names.add(name);
        values.add(System.getProperty(name));
        System.setProperty(name, nameValues[i + 1]);
      }
    }

    @Override
    public void close() {
      CanopySwitchPolicy.forget(player);
      for (int i = 0; i < names.size(); i++) {
        if (values.get(i) == null) System.clearProperty(names.get(i));
        else System.setProperty(names.get(i), values.get(i));
      }
    }
  }
}
