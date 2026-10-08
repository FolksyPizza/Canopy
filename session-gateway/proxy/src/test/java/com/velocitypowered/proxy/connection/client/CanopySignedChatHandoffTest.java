/* Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later */
package com.velocitypowered.proxy.connection.client;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.Unpooled;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CanopySignedChatHandoffTest {
  @Test void missingKeyFailsBeforeActivation() {
    String old = System.getProperty("canopy.chatHandoff.keyFile");
    System.clearProperty("canopy.chatHandoff.keyFile");
    try {
      var bridge = new CanopySignedChatHandoff(mock(ConnectedPlayer.class));
      assertTrue(bridge.capture(mock(VelocityServerConnection.class)).isCompletedExceptionally());
      assertFalse(bridge.active());
    } finally { if (old != null) System.setProperty("canopy.chatHandoff.keyFile", old); }
  }
  @Test void reservedForeignPacketsAreConsumedWithoutForwarding() {
    var bridge = new CanopySignedChatHandoff(mock(ConnectedPlayer.class));
    var packet = new PluginMessagePacket(ChatStateFrame.CHANNEL, Unpooled.buffer().writeByte(1));
    try { assertTrue(bridge.receive(mock(VelocityServerConnection.class), packet)); }
    finally { packet.release(); }
    assertFalse(bridge.active());
  }
  @Test void disconnectReleasesRetainedClientInputExactlyOnce() throws Exception {
    var bridge = new CanopySignedChatHandoff(mock(ConnectedPlayer.class));
    var active = CanopySignedChatHandoff.class.getDeclaredField("active");
    active.setAccessible(true); active.set(bridge, true);
    var packet = Unpooled.buffer().writeByte(1);
    assertTrue(bridge.bufferInput(packet)); assertEquals(2, packet.refCnt());
    var cut = CanopySignedChatHandoff.class.getDeclaredField("sourceCut");
    cut.setAccessible(true); cut.set(bridge, true);
    bridge.disconnect(); assertEquals(1, packet.refCnt());
    assertFalse(cut.getBoolean(bridge));
    bridge.disconnect(); assertEquals(1, packet.refCnt()); packet.release();
  }
  @Test void gameplayRawPacketsRemainOnExistingHandoverPath() {
    var player = mock(ConnectedPlayer.class);
    when(player.getProtocolVersion()).thenReturn(com.velocitypowered.api.network.ProtocolVersion.getProtocolVersion(774));
    var bridge = new CanopySignedChatHandoff(player);
    var packet = Unpooled.buffer().writeByte(0x1d);
    assertFalse(bridge.bufferRawChatInput(packet)); assertEquals(1, packet.refCnt()); packet.release();
  }
  @Test void inputBoundAbortsAndReleasesAllRetainedPackets() throws Exception {
    var player = mock(ConnectedPlayer.class);
    when(player.getConnection()).thenReturn(mock(com.velocitypowered.proxy.connection.MinecraftConnection.class));
    var bridge = new CanopySignedChatHandoff(player);
    var active = CanopySignedChatHandoff.class.getDeclaredField("active");
    active.setAccessible(true); active.set(bridge, true);
    var packet = Unpooled.buffer().writeZero(ChatStateFrame.MAX_TOTAL);
    assertTrue(bridge.bufferInput(packet)); assertEquals(2, packet.refCnt());
    var overflow = Unpooled.buffer().writeByte(1);
    assertFalse(bridge.bufferInput(overflow));
    assertFalse(bridge.active()); assertEquals(1, packet.refCnt()); assertEquals(1, overflow.refCnt());
    packet.release(); overflow.release();
  }
  @Test void timeoutFailureReleasesWithoutLeakingOrReusingInput() throws Exception {
    var player = mock(ConnectedPlayer.class);
    when(player.getConnection()).thenReturn(mock(com.velocitypowered.proxy.connection.MinecraftConnection.class));
    var bridge = new CanopySignedChatHandoff(player);
    var active = CanopySignedChatHandoff.class.getDeclaredField("active");
    active.setAccessible(true); active.set(bridge, true);
    var packet = Unpooled.buffer().writeByte(1);
    bridge.bufferInput(packet);
    bridge.abort(new IllegalStateException("Chat handoff timed out"));
    assertFalse(bridge.active()); assertEquals(1, packet.refCnt());
    bridge.abort(new IllegalStateException("Second timeout")); assertEquals(1, packet.refCnt());
    packet.release();
  }
  @Test void restoredSuccessCannotDetachAfterTimeoutOrSourceReplacement() throws Exception {
    var player = mock(ConnectedPlayer.class);
    when(player.getConnection()).thenReturn(mock(com.velocitypowered.proxy.connection.MinecraftConnection.class));
    var source = mock(VelocityServerConnection.class);
    var destination = mock(VelocityServerConnection.class);
    when(source.isActive()).thenReturn(true); when(destination.isActive()).thenReturn(true);
    when(player.getConnectedServer()).thenReturn(source);
    var bridge = new CanopySignedChatHandoff(player);
    for (var entry : java.util.Map.of("active", (Object) true, "source", source, "destination", destination,
        "restored", java.util.concurrent.CompletableFuture.completedFuture(null)).entrySet()) {
      var field = CanopySignedChatHandoff.class.getDeclaredField(entry.getKey());
      field.setAccessible(true); field.set(bridge, entry.getValue());
    }
    assertTrue(bridge.canCommit(destination));
    when(player.getConnectedServer()).thenReturn(mock(VelocityServerConnection.class));
    assertFalse(bridge.canCommit(destination));
    when(player.getConnectedServer()).thenReturn(source);
    var active = CanopySignedChatHandoff.class.getDeclaredField("active"); active.setAccessible(true); active.set(bridge, false);
    assertFalse(bridge.canCommit(destination));
    bridge.disconnect();
  }
  @Test void largeSignedAndUnsignedCommandsUseActualEncodedSize() throws Exception {
    var protocol = com.velocitypowered.api.network.ProtocolVersion.getProtocolVersion(774);
    for (boolean signed : new boolean[] {false, true}) {
      var player = mock(ConnectedPlayer.class);
      when(player.getConnection()).thenReturn(mock(com.velocitypowered.proxy.connection.MinecraftConnection.class));
      when(player.getProtocolVersion()).thenReturn(protocol);
      var bridge = new CanopySignedChatHandoff(player);
      var active = CanopySignedChatHandoff.class.getDeclaredField("active"); active.setAccessible(true); active.set(bridge, true);
      var wire = Unpooled.buffer();
      com.velocitypowered.proxy.protocol.ProtocolUtils.writeString(wire, "x".repeat(30000));
      com.velocitypowered.proxy.protocol.packet.chat.session.SessionPlayerCommandPacket command;
      if (signed) {
        wire.writeLong(System.currentTimeMillis()).writeLong(1);
        com.velocitypowered.proxy.protocol.ProtocolUtils.writeVarInt(wire, 1);
        com.velocitypowered.proxy.protocol.ProtocolUtils.writeString(wire, "target"); wire.writeZero(256);
        com.velocitypowered.proxy.protocol.ProtocolUtils.writeVarInt(wire, 0); wire.writeZero(4);
        command = new com.velocitypowered.proxy.protocol.packet.chat.session.SessionPlayerCommandPacket();
      } else command = new com.velocitypowered.proxy.protocol.packet.chat.session.UnsignedPlayerCommandPacket();
      command.decode(wire, com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.SERVERBOUND, protocol); wire.release();
      assertTrue(CanopySignedChatHandoff.packetSize(command,
          com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.SERVERBOUND, protocol, ChatStateFrame.MAX_TOTAL) > 30000);
      for (int i = 0; i < 8; i++) assertTrue(bridge.bufferInput(command));
      assertFalse(bridge.bufferInput(command)); assertFalse(bridge.active());
    }
  }
  @Test void typedDeferredPlayerInfoUsesWireSizeAndEncoderCap() {
    var protocol = com.velocitypowered.api.network.ProtocolVersion.getProtocolVersion(774);
    var packet = new com.velocitypowered.proxy.protocol.packet.UpsertPlayerInfoPacket();
    var encoded = Unpooled.buffer();
    packet.encode(encoded, com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.CLIENTBOUND, protocol);
    assertEquals(encoded.readableBytes() + 5, CanopySignedChatHandoff.packetSize(packet,
        com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.CLIENTBOUND, protocol, 8388608));
    encoded.release();
    var enormous = mock(com.velocitypowered.proxy.protocol.MinecraftPacket.class);
    doAnswer(call -> { ((io.netty.buffer.ByteBuf) call.getArgument(0)).writeZero(8388610); return null; })
        .when(enormous).encode(any(), any(), any());
    assertThrows(IndexOutOfBoundsException.class, () -> CanopySignedChatHandoff.packetSize(enormous,
        com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.CLIENTBOUND, protocol, 8388608));
  }
  @Test void sizingDeferredPluginDoesNotConsumeItsPayload() {
    var packet = new PluginMessagePacket("example:channel", Unpooled.buffer().writeZero(10000));
    try {
      int index = packet.content().readerIndex();
      int size = CanopySignedChatHandoff.packetSize(packet,
          com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.CLIENTBOUND,
          com.velocitypowered.api.network.ProtocolVersion.getProtocolVersion(774), 8388608);
      assertTrue(size > 10000); assertEquals(index, packet.content().readerIndex());
      assertEquals(10000, packet.content().readableBytes());
    } finally { packet.release(); }
  }
  @Test void frameAuthenticationExpiryAndAssemblyRemainStrict() throws Exception {
    byte[] key = new byte[32]; byte[] body = new byte[70000];
    var binding = new ChatStateFrame(ChatStateFrame.CAPTURE, UUID.randomUUID(), UUID.randomUUID(),
        "first", "second", System.currentTimeMillis() + 10000, 0, 1, 0, new byte[0]);
    var fragments = binding.fragments(ChatStateFrame.SNAPSHOT, body);
    var assembly = new ChatStateFrame.Assembly(fragments.get(0));
    assertNull(assembly.add(fragments.get(1))); assertNull(assembly.add(fragments.get(0)));
    assertArrayEquals(body, assembly.add(fragments.get(2)));
    assertThrows(Exception.class, () -> assembly.add(fragments.get(0)));
    byte[] frame = fragments.get(0).encode(key); frame[20] ^= 1;
    assertThrows(Exception.class, () -> ChatStateFrame.decode(frame, key, System.currentTimeMillis()));
    assertThrows(Exception.class, () -> ChatStateFrame.decode(fragments.get(0).encode(key), key,
        binding.expiresAt() + 1));
  }
}
