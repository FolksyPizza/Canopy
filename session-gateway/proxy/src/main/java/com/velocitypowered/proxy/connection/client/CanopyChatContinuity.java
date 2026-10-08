/*
 * Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;

/** Retains the recipient's unsigned global chat counter across backend changes. */
public final class CanopyChatContinuity {
  private static final int PLAYER_CHAT = 0x3f;
  private int nextIndex;
  private int backendBase;

  /** A full configuration resets client chat state; a skipped configuration preserves it. */
  public void beginBackend(boolean configurationSkipped) {
    if (!configurationSkipped) {
      nextIndex = 0;
    }
    backendBase = nextIndex;
  }

  /** Only the recipient counter changes. Sender indices, signatures and payload stay intact. */
  public ByteBuf clientbound(ByteBuf input, int protocol) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(protocol);
    if (profile == null) {
      return input;
    }
    ByteBuf read = input.duplicate();
    if (profile.clientbound(ProtocolUtils.readVarInt(read)) != PLAYER_CHAT) {
      return input;
    }
    int fieldStart = read.readerIndex();
    int backendIndex = ProtocolUtils.readVarInt(read);
    if (backendIndex < 0) {
      throw new IllegalArgumentException("Negative backend chat index");
    }
    // An additive mapping preserves any actual gap/reordering within the backend stream.
    int clientIndex = Math.addExact(backendBase, backendIndex);
    nextIndex = Math.addExact(clientIndex, 1);
    if (clientIndex == backendIndex) {
      return input;
    }
    ByteBuf output = input.alloc().buffer(input.readableBytes() + 5);
    output.writeBytes(input, input.readerIndex(), fieldStart - input.readerIndex());
    ProtocolUtils.writeVarInt(output, clientIndex);
    output.writeBytes(input, read.readerIndex(), input.writerIndex() - read.readerIndex());
    return output;
  }
}
