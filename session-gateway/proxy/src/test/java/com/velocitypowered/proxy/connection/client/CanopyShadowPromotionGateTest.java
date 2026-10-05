/*
 * Copyright (C) 2018-2025 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CanopyShadowPromotionGateTest {

  @Test
  void onlyMatchingSourceTargetAndTransferCanAcknowledgeOnce() {
    Object source = new Object();
    UUID transfer = UUID.randomUUID();
    CanopyShadowPromotionGate gate = new CanopyShadowPromotionGate();
    assertTrue(gate.arm(source, "Shard-East", transfer, 10L));

    assertFalse(gate.acknowledge(new Object(), "shard-east", transfer, 11L));
    assertFalse(gate.acknowledge(source, "shard-west", transfer, 11L));
    assertFalse(gate.acknowledge(source, "shard-east", UUID.randomUUID(), 11L));
    assertFalse(gate.mayPromote());

    assertTrue(gate.acknowledge(source, "SHARD-EAST", transfer, 12L));
    assertTrue(gate.mayPromote());
    assertFalse(gate.acknowledge(source, "shard-east", transfer, 13L));
    assertTrue(gate.consumeAcknowledgement());
    assertFalse(gate.consumeAcknowledgement());
    assertFalse(gate.isPending());
  }

  @Test
  void timeoutAbortsTheTicketAndRejectsLateAcknowledgement() {
    Object source = new Object();
    UUID transfer = UUID.randomUUID();
    CanopyShadowPromotionGate gate = new CanopyShadowPromotionGate();
    assertTrue(gate.arm(source, "east", transfer, 1_000L));

    assertFalse(gate.expired(1_000L + CanopyShadowPromotionGate.TIMEOUT_NANOS - 1));
    long deadline = 1_000L + CanopyShadowPromotionGate.TIMEOUT_NANOS;
    assertTrue(gate.expired(deadline));
    assertFalse(gate.acknowledge(source, "east", transfer, deadline));
    assertFalse(gate.isPending());
    assertFalse(gate.mayPromote());
  }

  @Test
  void malformedOrUncorrelatedWireAcknowledgementsAreRejectedWithoutConsumingInput() {
    UUID transfer = UUID.randomUUID();
    byte[] target = "east".getBytes(StandardCharsets.UTF_8);
    ByteBuf request = Unpooled.buffer();
    request.writeShort(target.length).writeBytes(target).writeByte(1)
        .writeLong(transfer.getMostSignificantBits()).writeLong(transfer.getLeastSignificantBits());
    int requestStart = request.readerIndex();
    assertEquals(transfer, CanopyHandover.switchTransferId(request));
    assertEquals(requestStart, request.readerIndex());

    ByteBuf acknowledgement = Unpooled.buffer(16);
    acknowledgement.writeLong(transfer.getMostSignificantBits()).writeLong(transfer.getLeastSignificantBits());
    int acknowledgementStart = acknowledgement.readerIndex();
    assertEquals(transfer, CanopyHandover.acknowledgementTransferId(acknowledgement));
    assertEquals(acknowledgementStart, acknowledgement.readerIndex());

    ByteBuf truncated = Unpooled.buffer(15).writeZero(15);
    assertNull(CanopyHandover.acknowledgementTransferId(truncated));
    assertEquals(0, truncated.readerIndex());
    request.release();
    acknowledgement.release();
    truncated.release();
  }
}
