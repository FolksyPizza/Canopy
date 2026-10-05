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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.BackendConnectionPhases;
import com.velocitypowered.proxy.connection.backend.BackendConnectionPhase;
import com.velocitypowered.proxy.connection.backend.CanopyEntityRewriter;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.ProtocolUtils.Direction;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CanopyHandoverTest {
  @Test
  void bedrockMovementAllowListIdsAreRawAcrossEveryNativeProfile() {
    int[] allowed = {0x1d, 0x1e, 0x1f, 0x20, 0x29, 0x2a, 0x34};
    for (int protocol = 771; protocol <= 777; protocol++) {
      var profile = CanopyProtocolProfile.find(protocol);
      var version = ProtocolVersion.getProtocolVersion(protocol);
      var registry = StateRegistry.PLAY.getProtocolRegistry(Direction.SERVERBOUND, version);
      for (int canonical : allowed) {
        assertNull(registry.createPacket(profile.serverboundWire(canonical)),
            "allow-listed movement/input ID " + Integer.toHexString(canonical) + " must stay raw for " + protocol);
      }
      assertNull(registry.createPacket(profile.serverboundWire(0x21)),
          "vehicle movement is raw but remains outside the safe replay allow-list for " + protocol);
    }
  }

  @Test
  void bedrockVehicleAndUnrelatedRawPacketsAreConsumedWithoutBuffering() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = new CanopyHandover(fixture.player());
      ByteBuf vehicle = movement(CanopyProtocolProfile.find(776), 0x21, 33);
      ByteBuf unrelated = Unpooled.buffer();
      ProtocolUtils.writeVarInt(unrelated, 0x7f);
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        assertTrue(handover.onServerbound(vehicle));
        assertTrue(handover.onServerbound(unrelated));
        assertEquals(0, handover.bufferedInputCount());
        assertEquals(0, handover.bufferedInputBytes());
      } finally {
        vehicle.release();
        unrelated.release();
        handover.abandon();
      }
    }
  }

  @Test
  void bedrockMovementWaitsForDestinationPlayAndReplaysInOrder() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = new CanopyHandover(fixture.player());
      var profile = CanopyProtocolProfile.find(776);
      ByteBuf first = movement(profile, 0x1d, 11);
      ByteBuf second = movement(profile, 0x20, 22);
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        assertTrue(handover.onServerbound(first));
        assertTrue(handover.onServerbound(second));
        assertEquals(2, handover.bufferedInputCount());
        assertEquals(first.readableBytes() + second.readableBytes(), handover.bufferedInputBytes());

        when(fixture.player().getConnectedServer()).thenReturn(fixture.destination());
        handover.arrived();
        handover.ready();
        verify(fixture.destinationConnection(), never()).delayedWrite(any());
        assertEquals(2, handover.bufferedInputCount(), "input remains held until PLAY readiness");

        when(fixture.destinationPhase().consideredComplete()).thenReturn(true);
        when(fixture.destinationConnection().getState()).thenReturn(StateRegistry.PLAY);
        handover.ready();

        ArgumentCaptor<Object> replayCaptor = ArgumentCaptor.forClass(Object.class);
        verify(fixture.destinationConnection(), times(2)).delayedWrite(replayCaptor.capture());
        List<ByteBuf> replay = replayCaptor.getAllValues().stream().map(ByteBuf.class::cast).toList();
        try {
          assertEquals(List.of(0x1d, 0x20), replay.stream()
              .map(packet -> profile.serverbound(ProtocolUtils.readVarInt(packet))).toList());
          assertEquals(List.of((byte) 11, (byte) 22), replay.stream().map(ByteBuf::readByte).toList());
        } finally {
          replay.forEach(ByteBuf::release);
        }
        assertEquals(0, handover.bufferedInputCount());
        assertEquals(0, handover.bufferedInputBytes());
        assertFalse(handover.active());
      } finally {
        first.release();
        second.release();
      }
    }
  }

  private static Fixture fixture(UUID id, int protocol) {
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    MinecraftConnection clientConnection = mock(MinecraftConnection.class);
    ClientPlaySessionHandler clientHandler = mock(ClientPlaySessionHandler.class);
    CanopyEntityRewriter rewriter = new CanopyEntityRewriter();
    rewriter.enabled(false);
    when(player.getUniqueId()).thenReturn(id);
    when(player.getUsername()).thenReturn("BedrockFixture");
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
    when(player.getConnection()).thenReturn(clientConnection);
    when(clientConnection.getActiveSessionHandler()).thenReturn(clientHandler);
    when(clientHandler.getCanopyRewriter()).thenReturn(rewriter);
    when(clientHandler.getCanopyChunkView()).thenReturn(new CanopyChunkView());

    VelocityServerConnection source = backend("main");
    VelocityServerConnection destination = backend("hold");
    BackendConnectionPhase destinationPhase = mock(BackendConnectionPhase.class);
    when(destinationPhase.consideredComplete()).thenReturn(false);
    when(destination.getPhase()).thenReturn(destinationPhase);
    when(player.getConnectedServer()).thenReturn(source);
    return new Fixture(player, source, destination, source.getConnection(), destination.getConnection(),
        destinationPhase);
  }

  private static VelocityServerConnection backend(String name) {
    VelocityServerConnection backend = mock(VelocityServerConnection.class);
    MinecraftConnection connection = mock(MinecraftConnection.class);
    when(backend.getConnection()).thenReturn(connection);
    when(backend.getServerInfo()).thenReturn(new ServerInfo(name, new InetSocketAddress("127.0.0.1", 25565)));
    when(backend.getPhase()).thenReturn(BackendConnectionPhases.VANILLA);
    when(connection.isClosed()).thenReturn(false);
    when(connection.getState()).thenReturn(StateRegistry.PLAY);
    return backend;
  }

  private static ByteBuf movement(CanopyProtocolProfile profile, int canonicalId, int marker) {
    ByteBuf packet = Unpooled.buffer();
    ProtocolUtils.writeVarInt(packet, profile.serverboundWire(canonicalId));
    packet.writeByte(marker);
    return packet;
  }

  private static PropertyRestore enableBedrockTrial(UUID player) {
    PropertyRestore restore = new PropertyRestore(player, "canopy.mode", "SEAMLESS",
        "canopy.allowSeamless", "true", "canopy.experimentalBedrockSeamless", "true");
    CanopySwitchPolicy.markPlatform(player, CanopySwitchPolicy.Platform.BEDROCK);
    CanopySwitchPolicy.seamless(player, true);
    return restore;
  }

  private record Fixture(ConnectedPlayer player, VelocityServerConnection source,
                         VelocityServerConnection destination, MinecraftConnection sourceConnection,
                         MinecraftConnection destinationConnection, BackendConnectionPhase destinationPhase) { }

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
