package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.connection.backend.CanopyEntityRewriter;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.junit.jupiter.api.Test;

class CanopyProtocolProfileTest {
  private static final int[] PROTOCOLS = {771, 772, 773, 774, 775, 776, 777};

  private CanopyEntityRewriter rewriter(int protocol) {
    var rewriter = new CanopyEntityRewriter();
    // Bind the table even when NORMAL is selected by the test process.
    rewriter.active(ProtocolVersion.getProtocolVersion(protocol));
    rewriter.initSelf(10);
    rewriter.onSwitch(300);
    return rewriter;
  }

  private ByteBuf packet(int id, int... fields) {
    ByteBuf out = Unpooled.buffer();
    ProtocolUtils.writeVarInt(out, id);
    for (int field : fields) { ProtocolUtils.writeVarInt(out, field); }
    return out;
  }

  @Test void allReleasedTargetProtocolsHaveExplicitProfilesAndUnknownVersionsDoNot() {
    for (int protocol : PROTOCOLS) {
      assertTrue(ProtocolVersion.isSupported(protocol));
      assertNotNull(CanopyProtocolProfile.find(protocol));
    }
    assertNull(CanopyProtocolProfile.find(778));
    assertNull(CanopyProtocolProfile.find(770));
  }

  @Test void translationPreservesWireIdsAndMovesCollidingEntitiesInBothDirections() {
    for (int protocol : PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      var rewriter = rewriter(protocol);
      int spawn = profile.clientboundWire(0x01);
      ByteBuf input = packet(spawn, 300);
      ByteBuf output = rewriter.processClientbound(input);
      try {
        assertEquals(spawn, ProtocolUtils.readVarInt(output));
        assertEquals(10, ProtocolUtils.readVarInt(output));
      } finally { if (output != input) { output.release(); } input.release(); }
      int interact = profile.serverboundWire(0x19);
      input = packet(interact, 10);
      output = rewriter.processServerbound(input);
      try {
        assertEquals(interact, ProtocolUtils.readVarInt(output));
        assertEquals(300, ProtocolUtils.readVarInt(output));
      } finally { if (output != input) { output.release(); } input.release(); }
    }
  }

  @Test void damageTranslatesVictimAndBothOptionalCauseIds() {
    for (int protocol : PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      ByteBuf input = packet(profile.clientboundWire(0x19), 300, 7, 301, 11);
      input.writeBoolean(false);
      ByteBuf output = rewriter(protocol).processClientbound(input);
      try {
        assertEquals(profile.clientboundWire(0x19), ProtocolUtils.readVarInt(output));
        assertEquals(10, ProtocolUtils.readVarInt(output));
        assertEquals(7, ProtocolUtils.readVarInt(output));
        assertEquals(11, ProtocolUtils.readVarInt(output));
        assertEquals(301, ProtocolUtils.readVarInt(output));
        assertFalse(output.readBoolean());
      } finally { if (output != input) { output.release(); } input.release(); }
    }
  }

  @Test void generatedJoinStateUsesDestinationProtocolPacketIds() {
    for (int protocol : PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      List<ByteBuf> packets = rewriter(protocol).buildJoinState(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT, 0, 10, 8);
      int[] canonical = {0x26, 0x5d, 0x6d, 0x0e};
      try {
        for (int i = 0; i < canonical.length; i++) {
          assertEquals(profile.clientboundWire(canonical[i]), ProtocolUtils.readVarInt(packets.get(i)));
        }
      } finally { packets.forEach(ByteBuf::release); }
    }
  }

  @Test void newerVersionsUseTheirOwnIdsRatherThanOneHardcodedTable() {
    assertEquals(0x41, CanopyProtocolProfile.find(771).clientboundWire(0x46));
    assertEquals(0x46, CanopyProtocolProfile.find(774).clientboundWire(0x46));
    assertEquals(0x48, CanopyProtocolProfile.find(775).clientboundWire(0x46));
    assertEquals(0x49, CanopyProtocolProfile.find(777).clientboundWire(0x46));
  }

  @Test void newAttackAndSpectatorPacketsTranslateWithoutTouchingAbsentTargets() {
    for (int protocol : new int[]{775, 776, 777}) {
      var profile = CanopyProtocolProfile.find(protocol);
      for (int canonical : new int[]{0x900, 0x901}) {
        ByteBuf input = packet(profile.serverboundWire(canonical), canonical == 0x900 ? 10 : 11);
        ByteBuf output = rewriter(protocol).processServerbound(input);
        try {
          assertEquals(profile.serverboundWire(canonical), ProtocolUtils.readVarInt(output));
          assertEquals(canonical == 0x900 ? 300 : 301, ProtocolUtils.readVarInt(output));
        } finally { if (output != input) { output.release(); } input.release(); }
      }
    }
  }

  @Test void entityIdTranslationLeavesPlayerMovementPacketsByteForByteUntouched() {
    int[] movementPackets = {0x1d, 0x1e, 0x1f, 0x20};
    for (int protocol : PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      for (int canonical : movementPackets) {
        ByteBuf input = Unpooled.buffer();
        ProtocolUtils.writeVarInt(input, profile.serverboundWire(canonical));
        input.writeLong(0x0123456789abcdefL).writeFloat(0.25f).writeFloat(-0.5f).writeBoolean(true);
        byte[] expected = new byte[input.readableBytes()];
        input.getBytes(input.readerIndex(), expected);
        ByteBuf output = rewriter(protocol).processServerbound(input);
        try {
          assertSame(input, output, "movement packet must pass through for protocol " + protocol);
          byte[] actual = new byte[output.readableBytes()];
          output.getBytes(output.readerIndex(), actual);
          assertArrayEquals(expected, actual, "movement bytes must be unchanged for protocol " + protocol);
          assertEquals(0, output.readerIndex());
        } finally {
          if (output != input) { output.release(); }
          input.release();
        }
      }
    }
  }
}
