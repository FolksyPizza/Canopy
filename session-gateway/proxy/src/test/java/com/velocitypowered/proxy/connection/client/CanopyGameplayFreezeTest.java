package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.ProtocolUtils.Direction;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.ClientSettingsPacket;
import com.velocitypowered.proxy.protocol.packet.config.FinishedUpdatePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class CanopyGameplayFreezeTest {
  private static final int[] PROTOCOLS = {771, 772, 773, 774, 775, 776, 777};
  private static final int[] POSITION_IDS = {0x41, 0x41, 0x46, 0x46, 0x48, 0x48, 0x49};
  private static final int[] CHAT_ACK_IDS = {0x05, 0x05, 0x05, 0x05, 0x06, 0x06, 0x06};
  private static final int[] CHAT_IDS = {0x08, 0x08, 0x08, 0x08, 0x09, 0x09, 0x09};
  private static final int[] KEEP_ALIVE_IDS = {0x1b, 0x1b, 0x1b, 0x1b, 0x1c, 0x1c, 0x1c};
  private static final int[] VEHICLE_MOVE_IDS = {0x21, 0x21, 0x21, 0x21, 0x22, 0x22, 0x22};
  // Independent wire fixture: teleport VarInt, absolute xyz, zero deltas, yaw/pitch, no relative flags.
  private static final String POSITION_CORRECTION_BODY =
      "81a8d6b90740288000000000004054200000000000c01f00000000000000000000000000000000000000000000000000000000000043070000c1a0000000000000";

  @Test
  void freezeProfileMatrixFailsClosedAcrossVelocityNativeSupportRange() {
    int supportedProfiles = 0;
    for (ProtocolVersion version : ProtocolVersion.SUPPORTED_VERSIONS) {
      int protocol = version.getProtocol();
      boolean expected = protocol >= 771 && protocol <= 777;
      boolean actual = CanopyProtocolProfile.find(protocol) != null;
      assertEquals(expected, actual,
          "freeze profile matrix for Velocity-supported " + version.getVersionIntroducedIn()
              + " protocol " + protocol);
      if (actual) supportedProfiles++;
    }
    assertEquals(PROTOCOLS.length, supportedProfiles,
        "only native protocols 771–777 have an explicit freeze profile");
    assertNull(CanopyProtocolProfile.find(4), "1.7.2 must fail freeze preflight closed");
    assertNull(CanopyProtocolProfile.find(770), "1.21.5 must fail freeze preflight closed");
  }

  @Test
  void freezeTransportProfileMappingsAreExplicitForEverySupportedVersion() {
    for (int index = 0; index < PROTOCOLS.length; index++) {
      CanopyProtocolProfile profile = CanopyProtocolProfile.find(PROTOCOLS[index]);
      assertEquals(POSITION_IDS[index], profile.clientboundWire(0x46));
      assertEquals(CHAT_ACK_IDS[index], profile.serverboundWire(0x05));
      assertEquals(CHAT_IDS[index], profile.serverboundWire(0x08));
      assertEquals(0, profile.serverboundWire(0x00));
      assertEquals(0x0a + (PROTOCOLS[index] >= 775 ? 1 : 0),
          profile.serverboundWire(0x0a));
      assertEquals(0x0c + (PROTOCOLS[index] >= 775 ? 1 : 0),
          profile.serverboundWire(0x0c));
      assertEquals(0x2b + (PROTOCOLS[index] >= 775 ? 1 : 0),
          profile.serverboundWire(0x2b));
      assertEquals(0x2c + (PROTOCOLS[index] >= 775 ? 1 : 0),
          profile.serverboundWire(0x2c));
      assertEquals(KEEP_ALIVE_IDS[index], profile.serverboundWire(0x1b));
      assertEquals(profile.serverboundWire(0x21), VEHICLE_MOVE_IDS[index]);
      assertEquals(0x1d + (PROTOCOLS[index] >= 775 ? 1 : 0),
          profile.serverboundWire(0x1d));
      assertEquals(-1, profile.serverbound(0x7f));
      assertNull(StateRegistry.PLAY.getProtocolRegistry(Direction.CLIENTBOUND,
          ProtocolVersion.getProtocolVersion(PROTOCOLS[index])).createPacket(profile.clientboundWire(0x46)));
    }
  }

  @Test
  void supportedMovementPacketsAreRawUnknownPacketsBeforeVelocityDispatch() {
    int[] movementIds = {0x1d, 0x1e, 0x1f, 0x20, 0x21};
    for (int protocol : PROTOCOLS) {
      CanopyProtocolProfile profile = CanopyProtocolProfile.find(protocol);
      ProtocolVersion version = ProtocolVersion.getProtocolVersion(protocol);
      assertTrue(version.isSupported(), "protocol " + protocol);
      var registry = StateRegistry.PLAY.getProtocolRegistry(Direction.SERVERBOUND, version);
      for (int canonical : movementIds) {
        int wireId = profile.serverboundWire(canonical);
        assertNull(registry.createPacket(wireId),
            "movement wire packet " + Integer.toHexString(wireId) + " should be raw for " + protocol);
      }
    }
  }

  @Test
  void positionCorrectionAcceptsSupportedRelativeFlagsAndAppliesPositionBits() {
    CanopyGameplayFreeze.Anchor previous = new CanopyGameplayFreeze.Anchor(10.0, 20.0, 30.0, 90.0f, -10.0f);
    ByteBuf packet = relativePositionCorrection(776, 91, 1.25, -2.0, 3.0, 0.5, -0.25,
        2.0, 15.0f, 5.0f, 0x1ff);
    try {
      CanopyGameplayFreeze.PositionCorrection parsed = CanopyGameplayFreeze.readPositionCorrection(
          CanopyProtocolProfile.find(776), previous, packet);
      assertNotNull(parsed);
      assertEquals(91, parsed.teleportId());
      assertEquals(11.25, parsed.anchor().x());
      assertEquals(18.0, parsed.anchor().y());
      assertEquals(33.0, parsed.anchor().z());
      assertEquals(105.0f, parsed.anchor().yaw());
      assertEquals(-5.0f, parsed.anchor().pitch());
      assertEquals(0x1ffL, parsed.relativeFlags());
      assertEquals(0, packet.readerIndex());
    } finally {
      packet.release();
    }
  }

  @Test
  void positionCorrectionRejectsUnknownRelativeFlags() {
    ByteBuf packet = relativePositionCorrection(776, 92, 1.0, 2.0, 3.0, 0.0, 0.0,
        0.0, 0.0f, 0.0f, 0x200);
    try {
      assertNull(CanopyGameplayFreeze.readPositionCorrection(CanopyProtocolProfile.find(776),
          new CanopyGameplayFreeze.Anchor(0.0, 0.0, 0.0, 0.0f, 0.0f), packet));
      assertEquals(0, packet.readerIndex());
    } finally {
      packet.release();
    }
  }

  @Test
  void rawMovementUpdatesFreezeAnchorToLatestCoordinates() {
    for (int protocol : PROTOCOLS) {
      CanopyGameplayFreeze.Anchor initial = new CanopyGameplayFreeze.Anchor(10.0, 64.0, -5.0, 45.0f, 2.0f);
      CanopyGameplayFreeze.Anchor moved = readRawMovement(protocol, 0x1d, out ->
          out.writeDouble(13.5).writeDouble(72.0).writeDouble(-2.25).writeByte(1), initial);
      assertEquals(13.5, moved.x());
      assertEquals(72.0, moved.y());
      assertEquals(-2.25, moved.z());
      assertEquals(45.0f, moved.yaw());

      ByteBuf correction = Unpooled.buffer();
      try {
        CanopyGameplayFreeze.writeCorrection(correction, CanopyProtocolProfile.find(protocol), 2_000_000_010,
            moved.x(), moved.y(), moved.z(), moved.yaw(), moved.pitch());
        ProtocolUtils.readVarInt(correction); // packet id
        assertEquals(2_000_000_010, ProtocolUtils.readVarInt(correction));
        assertEquals(moved.x(), correction.readDouble());
        assertEquals(moved.y(), correction.readDouble());
        assertEquals(moved.z(), correction.readDouble());
      } finally {
        correction.release();
      }
    }

    CanopyGameplayFreeze.Anchor moved = new CanopyGameplayFreeze.Anchor(13.5, 72.0, -2.25, 45.0f, 2.0f);
    CanopyGameplayFreeze.Anchor rotated = readRawMovement(774, 0x1f, out ->
        out.writeFloat(130.0f).writeFloat(-12.0f).writeByte(0), moved);
    assertEquals(13.5, rotated.x());
    assertEquals(130.0f, rotated.yaw());
    assertEquals(-12.0f, rotated.pitch());
  }

  @Test
  void rawPacketPolicyAllowsOnlyChatAndProtocolMaintenanceTraffic() {
    assertEquals(CanopyGameplayFreeze.RawAction.TELEPORT_ACK,
        CanopyGameplayFreeze.rawAction(0x00));
    assertEquals(CanopyGameplayFreeze.RawAction.CHAT,
        CanopyGameplayFreeze.rawAction(0x08));
    assertEquals(CanopyGameplayFreeze.RawAction.TRANSPORT_ACK,
        CanopyGameplayFreeze.rawAction(0x05));
    assertEquals(CanopyGameplayFreeze.RawAction.TRANSPORT_ACK,
        CanopyGameplayFreeze.rawAction(0x1b));
    assertEquals(CanopyGameplayFreeze.RawAction.MOVEMENT,
        CanopyGameplayFreeze.rawAction(0x1d));
    assertEquals(CanopyGameplayFreeze.RawAction.MOVEMENT,
        CanopyGameplayFreeze.rawAction(0x1e));
    assertEquals(CanopyGameplayFreeze.RawAction.MOVEMENT,
        CanopyGameplayFreeze.rawAction(0x1f));
    assertEquals(CanopyGameplayFreeze.RawAction.MOVEMENT,
        CanopyGameplayFreeze.rawAction(0x20));
    assertEquals(CanopyGameplayFreeze.RawAction.MOVEMENT,
        CanopyGameplayFreeze.rawAction(0x21));
    assertEquals(CanopyGameplayFreeze.RawAction.TRANSPORT_ACK,
        CanopyGameplayFreeze.rawAction(0x2b));
    assertEquals(CanopyGameplayFreeze.RawAction.DROP,
        CanopyGameplayFreeze.rawAction(0x0a));
    assertEquals(CanopyGameplayFreeze.RawAction.DROP,
        CanopyGameplayFreeze.rawAction(0x0c));
    assertEquals(CanopyGameplayFreeze.RawAction.DROP,
        CanopyGameplayFreeze.rawAction(0x2c));
    assertEquals(CanopyGameplayFreeze.RawAction.DROP,
        CanopyGameplayFreeze.rawAction(-1));
    assertEquals(CanopyGameplayFreeze.RawAction.DROP,
        CanopyGameplayFreeze.rawAction(0x3f));
  }

  @Test
  void configurationAcknowledgementAndClientSettingsPassWhileFrozen() {
    assertTrue(CanopyGameplayFreeze.allowedTyped(FinishedUpdatePacket.INSTANCE),
        "backend configuration completion must not strand a frozen handover");
    assertTrue(CanopyGameplayFreeze.allowedTyped(new ClientSettingsPacket()),
        "client preference updates are control state, not gameplay input");
  }

  @Test
  void rawAndTypedChatMustNotContainACommand() {
    assertTrue(CanopyGameplayFreeze.plainChat("maintenance progress"));
    assertFalse(CanopyGameplayFreeze.plainChat("/spawn"));
    assertTrue(CanopyGameplayFreeze.plainChat(""));

    ByteBuf raw = Unpooled.buffer();
    ProtocolUtils.writeString(raw, "maintenance progress");
    assertTrue(CanopyGameplayFreeze.rawChatIsPlainChat(raw));
    raw.clear();
    ProtocolUtils.writeString(raw, "/spawn");
    assertFalse(CanopyGameplayFreeze.rawChatIsPlainChat(raw));
    raw.release();
  }

  @Test
  void syntheticPositionCorrectionUsesEachProfilesPositionIdAndWireLayout() {
    for (int index = 0; index < PROTOCOLS.length; index++) {
      CanopyProtocolProfile profile = CanopyProtocolProfile.find(PROTOCOLS[index]);
      ByteBuf independentCorrection = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(
          String.format("%02x", POSITION_IDS[index]) + POSITION_CORRECTION_BODY));
      try {
        CanopyGameplayFreeze.PositionCorrection parsed = CanopyGameplayFreeze.readPositionCorrection(
            profile, null, independentCorrection);
        assertNotNull(parsed);
        assertEquals(2_000_000_001, parsed.teleportId());
        assertEquals(12.25, parsed.anchor().x());
        assertEquals(80.5, parsed.anchor().y());
        assertEquals(-7.75, parsed.anchor().z());
        assertEquals(135.0f, parsed.anchor().yaw());
        assertEquals(-20.0f, parsed.anchor().pitch());
        assertEquals(0, independentCorrection.readerIndex());
      } finally {
        independentCorrection.release();
      }

      ByteBuf correction = Unpooled.buffer();
      try {
        CanopyGameplayFreeze.writeCorrection(correction, profile, 2_000_000_001,
            12.25, 80.5, -7.75, 135.0f, -20.0f);
        assertEquals(String.format("%02x", POSITION_IDS[index]) + POSITION_CORRECTION_BODY,
            ByteBufUtil.hexDump(correction));
        assertEquals(POSITION_IDS[index], ProtocolUtils.readVarInt(correction));
        assertEquals(2_000_000_001, ProtocolUtils.readVarInt(correction));
        assertEquals(12.25, correction.readDouble());
        assertEquals(80.5, correction.readDouble());
        assertEquals(-7.75, correction.readDouble());
        assertEquals(0.0, correction.readDouble());
        assertEquals(0.0, correction.readDouble());
        assertEquals(0.0, correction.readDouble());
        assertEquals(135.0f, correction.readFloat());
        assertEquals(-20.0f, correction.readFloat());
        assertEquals(0L, correction.readUnsignedInt());
        assertFalse(correction.isReadable());
      } finally {
        correction.release();
      }
    }
  }

  private static ByteBuf relativePositionCorrection(int protocol, int teleportId,
      double x, double y, double z, double deltaX, double deltaY, double deltaZ,
      float yaw, float pitch, int relativeFlags) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(protocol);
    ByteBuf packet = Unpooled.buffer();
    ProtocolUtils.writeVarInt(packet, profile.clientboundWire(0x46));
    ProtocolUtils.writeVarInt(packet, teleportId);
    packet.writeDouble(x).writeDouble(y).writeDouble(z);
    packet.writeDouble(deltaX).writeDouble(deltaY).writeDouble(deltaZ);
    packet.writeFloat(yaw).writeFloat(pitch).writeInt(relativeFlags);
    return packet;
  }

  private static CanopyGameplayFreeze.Anchor readRawMovement(int protocol, int canonical,
      java.util.function.Consumer<ByteBuf> payload, CanopyGameplayFreeze.Anchor anchor) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(protocol);
    ByteBuf packet = Unpooled.buffer();
    try {
      ProtocolUtils.writeVarInt(packet, profile.serverboundWire(canonical));
      payload.accept(packet);
      int wireId = ProtocolUtils.readVarInt(packet);
      int decodedCanonical = profile.serverbound(wireId);
      return CanopyGameplayFreeze.readMovement(anchor, decodedCanonical, packet);
    } finally {
      packet.release();
    }
  }
}
