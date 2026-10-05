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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.BackendConnectionPhases;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ClientPlaySessionHandlerBedrockHandoverTest {
  @Test
  void movementIsBufferedBeforeNullConnectedServerEarlyReturn() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = fixture.handler().getCanopyHandover();
      ByteBuf movement = movement(776, 0x1d, 73);
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        when(fixture.player().getConnectedServer()).thenReturn(null);

        fixture.handler().handleUnknown(movement);

        assertEquals(1, handover.bufferedInputCount());
        assertEquals(movement.readableBytes(), handover.bufferedInputBytes());
        verify(fixture.sourceConnection(), times(1)).write(org.mockito.ArgumentMatchers.any());
      } finally {
        movement.release();
        handover.abandon();
      }
    }
  }

  @Test
  void byteOverflowCancelsToSourceAndForwardsTheTriggeringMovement() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = fixture.handler().getCanopyHandover();
      ByteBuf prefix = Unpooled.buffer(CanopyHandover.MAX_BUFFERED_BYTES);
      ByteBuf trigger = movement(776, 0x1e, 91);
      ProtocolUtils.writeVarInt(prefix, CanopyProtocolProfile.find(776).serverboundWire(0x1d));
      prefix.writeZero(CanopyHandover.MAX_BUFFERED_BYTES - prefix.readableBytes());
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        fixture.handler().handleUnknown(prefix);
        assertEquals(1, handover.bufferedInputCount());
        assertEquals(CanopyHandover.MAX_BUFFERED_BYTES, handover.bufferedInputBytes());

        fixture.handler().handleUnknown(trigger);

        assertEquals(0, handover.bufferedInputCount());
        assertEquals(0, handover.bufferedInputBytes());
        assertFalse(handover.active(), "overflow fallback resets the handover");
        ArgumentCaptor<Object> replayCaptor = ArgumentCaptor.forClass(Object.class);
        verify(fixture.sourceConnection()).delayedWrite(replayCaptor.capture());
        ByteBuf replayedPrefix = (ByteBuf) replayCaptor.getValue();
        assertArrayEquals(ByteBufUtil.getBytes(prefix), ByteBufUtil.getBytes(replayedPrefix));

        ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(fixture.sourceConnection(), times(3)).write(writes.capture());
        List<Object> written = writes.getAllValues();
        assertEquals(CanopyHandover.CUT_CHANNEL, ((PluginMessagePacket) written.get(0)).getChannel());
        assertEquals(CanopyHandover.CANCEL_CHANNEL, ((PluginMessagePacket) written.get(1)).getChannel());
        assertArrayEquals(ByteBufUtil.getBytes(trigger), ByteBufUtil.getBytes((ByteBuf) written.get(2)));
        ReferenceCountUtil.release(replayedPrefix);
        ReferenceCountUtil.release(written.get(0));
        ReferenceCountUtil.release(written.get(1));
        ReferenceCountUtil.release(written.get(2));
      } finally {
        prefix.release();
        trigger.release();
        handover.abandon();
      }
    }
  }

  @Test
  void packetCountOverflowCancelsToSourceAndReleasesTheQueue() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = fixture.handler().getCanopyHandover();
      ByteBuf movement = movement(776, 0x1d, 23);
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        for (int index = 0; index < CanopyHandover.MAX_BUFFERED; index++) {
          fixture.handler().handleUnknown(movement);
        }
        assertEquals(CanopyHandover.MAX_BUFFERED, handover.bufferedInputCount());
        assertTrue(handover.bufferedInputBytes() < CanopyHandover.MAX_BUFFERED_BYTES);

        fixture.handler().handleUnknown(movement);

        assertFalse(handover.active());
        assertEquals(0, handover.bufferedInputCount());
        assertEquals(0, handover.bufferedInputBytes());
        verify(fixture.sourceConnection(), times(CanopyHandover.MAX_BUFFERED))
            .delayedWrite(org.mockito.ArgumentMatchers.any());
      } finally {
        movement.release();
        handover.abandon();
      }
    }
  }

  @Test
  void disconnectReleasesBufferedMovement() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = fixture.handler().getCanopyHandover();
      ByteBuf movement = movement(776, 0x1d, 19);
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        fixture.handler().handleUnknown(movement);
        assertEquals(1, handover.bufferedInputCount());

        fixture.handler().disconnected();

        assertFalse(handover.active());
        assertEquals(0, handover.bufferedInputCount());
        assertEquals(0, handover.bufferedInputBytes());
      } finally {
        movement.release();
        handover.abandon();
      }
    }
  }

  @Test
  void timedOutCutRemainsBoundedUntilExplicitCancellation() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableBedrockTrial(id)) {
      Fixture fixture = fixture(id, 776);
      CanopyHandover handover = fixture.handler().getCanopyHandover();
      ByteBuf first = movement(776, 0x1d, 20);
      ByteBuf afterTimeout = movement(776, 0x20, 21);
      try {
        assertTrue(handover.beginBedrockMovementBuffer(fixture.source()));
        fixture.handler().handleUnknown(first);

        Field stateSince = CanopyHandover.class.getDeclaredField("stateSince");
        stateSince.setAccessible(true);
        stateSince.setLong(handover, System.currentTimeMillis() - 20_000L);
        fixture.handler().handleUnknown(afterTimeout);

        assertTrue(handover.active(), "timeout is diagnostic and does not assume destination state was applied");
        assertEquals(2, handover.bufferedInputCount());
        assertTrue(handover.bufferedInputBytes() <= CanopyHandover.MAX_BUFFERED_BYTES);

        handover.cancel();

        assertFalse(handover.active());
        assertEquals(0, handover.bufferedInputCount());
        assertEquals(0, handover.bufferedInputBytes());
      } finally {
        first.release();
        afterTimeout.release();
        handover.abandon();
      }
    }
  }

  @Test
  void ordinaryJavaRawMovementStillForwardsDirectly() throws Exception {
    UUID id = UUID.randomUUID();
    try (PropertyRestore ignored = enableJavaSession(id)) {
      Fixture fixture = fixture(id, 776);
      ByteBuf movement = movement(776, 0x1d, 17);
      try {
        fixture.handler().handleUnknown(movement);

        ArgumentCaptor<Object> write = ArgumentCaptor.forClass(Object.class);
        verify(fixture.sourceConnection()).write(write.capture());
        ByteBuf forwarded = (ByteBuf) write.getValue();
        assertArrayEquals(ByteBufUtil.getBytes(movement), ByteBufUtil.getBytes(forwarded));
        assertEquals(0, fixture.handler().getCanopyHandover().bufferedInputCount());
        ReferenceCountUtil.release(forwarded);
      } finally {
        movement.release();
      }
    }
  }

  private static Fixture fixture(UUID id, int protocol) {
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    VelocityServer server = mock(VelocityServer.class);
    MinecraftConnection clientConnection = mock(MinecraftConnection.class);
    when(player.getUniqueId()).thenReturn(id);
    when(player.getUsername()).thenReturn("FixtureClient");
    when(player.getConnection()).thenReturn(clientConnection);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
    when(clientConnection.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
    ClientPlaySessionHandler handler = new ClientPlaySessionHandler(server, player);
    when(clientConnection.getActiveSessionHandler()).thenReturn(handler);

    VelocityServerConnection source = mock(VelocityServerConnection.class);
    MinecraftConnection sourceConnection = mock(MinecraftConnection.class);
    when(source.getConnection()).thenReturn(sourceConnection);
    when(source.getServerInfo()).thenReturn(new ServerInfo("main",
        new InetSocketAddress("127.0.0.1", 25565)));
    when(source.getPhase()).thenReturn(BackendConnectionPhases.VANILLA);
    when(sourceConnection.isClosed()).thenReturn(false);
    when(sourceConnection.getState()).thenReturn(StateRegistry.PLAY);
    when(player.getConnectedServer()).thenReturn(source);
    return new Fixture(player, source, sourceConnection, handler);
  }

  private static ByteBuf movement(int protocol, int canonicalId, int marker) {
    ByteBuf packet = Unpooled.buffer();
    ProtocolUtils.writeVarInt(packet, CanopyProtocolProfile.find(protocol).serverboundWire(canonicalId));
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

  private static PropertyRestore enableJavaSession(UUID player) {
    PropertyRestore restore = new PropertyRestore(player, "canopy.mode", "SEAMLESS",
        "canopy.allowSeamless", "false", "canopy.experimentalBedrockSeamless", "false");
    CanopySwitchPolicy.markPlatform(player, CanopySwitchPolicy.Platform.JAVA);
    CanopySwitchPolicy.seamless(player, true);
    return restore;
  }

  private record Fixture(ConnectedPlayer player, VelocityServerConnection source,
                         MinecraftConnection sourceConnection, ClientPlaySessionHandler handler) { }

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
