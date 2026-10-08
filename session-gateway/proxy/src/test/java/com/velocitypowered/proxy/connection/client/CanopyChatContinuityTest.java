/*
 * Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;

import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class CanopyChatContinuityTest {
  private ByteBuf packet(int protocol, int index) {
    ByteBuf packet = Unpooled.buffer();
    ProtocolUtils.writeVarInt(packet, CanopyProtocolProfile.find(protocol).clientboundWire(0x3f));
    ProtocolUtils.writeVarInt(packet, index);
    // Represents the signed body; the bridge must not change even a single byte after the counter.
    for (int i = 0; i < 300; i++) packet.writeByte(i);
    return packet;
  }

  private void expect(CanopyChatContinuity bridge, int protocol, int backend, int client) {
    ByteBuf input = packet(protocol, backend);
    int originalReader = input.readerIndex();
    ByteBuf output = bridge.clientbound(input, protocol);
    try {
      assertEquals(originalReader, input.readerIndex());
      ByteBuf read = output.duplicate();
      assertEquals(CanopyProtocolProfile.find(protocol).clientboundWire(0x3f), ProtocolUtils.readVarInt(read));
      assertEquals(client, ProtocolUtils.readVarInt(read));
      assertEquals(300, read.readableBytes());
      for (int i = 0; i < 300; i++) assertEquals((byte) i, read.readByte());
    } finally {
      if (output != input) output.release();
      input.release();
    }
  }

  @Test void repeatedSwitchesRetainGlobalCounterOnEveryNativeProfile() {
    for (int protocol : CanopyProtocolProfile.supportedProtocols()) {
      var bridge = new CanopyChatContinuity();
      bridge.beginBackend(false);
      for (int index = 0; index < 128; index++) expect(bridge, protocol, index, index);
      bridge.beginBackend(true);
      expect(bridge, protocol, 0, 128);
      expect(bridge, protocol, 1, 129);
      bridge.beginBackend(true);
      bridge.beginBackend(true); // empty backend, followed by another switch
      expect(bridge, protocol, 0, 130);
      bridge.beginBackend(false);
      expect(bridge, protocol, 0, 0);
    }
  }

  @Test void backendGapIsNotHiddenByCounterTranslation() {
    var bridge = new CanopyChatContinuity();
    bridge.beginBackend(false);
    expect(bridge, 774, 0, 0);
    bridge.beginBackend(true);
    expect(bridge, 774, 0, 1);
    expect(bridge, 774, 2, 3); // actual missing packet remains visible to the client
  }

  @Test void nonChatAndUnknownProtocolsAreUntouched() {
    var bridge = new CanopyChatContinuity();
    ByteBuf input = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
    try {
      assertSame(input, bridge.clientbound(input, 774));
      assertSame(input, bridge.clientbound(input, 778));
      assertEquals(0, input.readerIndex());
    } finally { input.release(); }
  }
}
