package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.packet.config.KnownPacksPacket;
import com.velocitypowered.proxy.protocol.packet.config.RegistrySyncPacket;
import com.velocitypowered.proxy.protocol.packet.config.TagsUpdatePacket;
import io.netty.buffer.Unpooled;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CanopyConfigurationTest {
  private io.netty.buffer.ByteBuf registry(byte value, boolean reverseKeys, boolean reverseEntries) {
    var buffer = Unpooled.buffer();
    ProtocolUtils.writeString(buffer,"minecraft:test"); ProtocolUtils.writeVarInt(buffer,2);
    for (String entry : reverseEntries ? java.util.List.of("second","first") : java.util.List.of("first","second")) {
      ProtocolUtils.writeString(buffer,"minecraft:"+entry); buffer.writeBoolean(true); buffer.writeByte(10);
      for (String key : reverseKeys ? java.util.List.of("b","a") : java.util.List.of("a","b")) {
        buffer.writeByte(3); buffer.writeShort(1); buffer.writeByte(key.charAt(0)); buffer.writeInt(key.equals("a") ? value : 17);
      }
      buffer.writeByte(0);
    }
    return buffer;
  }
  private String measured(io.netty.buffer.ByteBuf input) {
    var packet = new RegistrySyncPacket();
    try {
      packet.decode(input, ProtocolUtils.Direction.CLIENTBOUND, ProtocolVersion.MINECRAFT_1_21_11);
      int before=packet.content().readerIndex();
      var config = new CanopyConfiguration(); config.record(packet,ProtocolVersion.MINECRAFT_1_21_11);
      assertEquals(before,packet.content().readerIndex());
      config.record(new TagsUpdatePacket(Map.of("minecraft:block",Map.of("minecraft:test",new int[]{1}))),ProtocolVersion.MINECRAFT_1_21_11);
      return config.finish();
    } finally { packet.release(); input.release(); }
  }
  private KnownPacksPacket knownPacks(String id) {
    var input = Unpooled.buffer();
    try {
      ProtocolUtils.writeVarInt(input, 1);
      ProtocolUtils.writeString(input, "minecraft");
      ProtocolUtils.writeString(input, id);
      ProtocolUtils.writeString(input, "1");
      var packet = new KnownPacksPacket();
      packet.decode(input, ProtocolUtils.Direction.CLIENTBOUND, ProtocolVersion.MINECRAFT_1_21_11);
      return packet;
    } finally { input.release(); }
  }
  private String measuredWithKnownPacks(String id, int copies) {
    var input = registry((byte) 1, false, false);
    var packet = new RegistrySyncPacket();
    try {
      packet.decode(input, ProtocolUtils.Direction.CLIENTBOUND, ProtocolVersion.MINECRAFT_1_21_11);
      var config = new CanopyConfiguration();
      config.record(packet, ProtocolVersion.MINECRAFT_1_21_11);
      config.record(new TagsUpdatePacket(Map.of("minecraft:block", Map.of("minecraft:test", new int[] {1}))),
          ProtocolVersion.MINECRAFT_1_21_11);
      var knownPacks = knownPacks(id);
      for (int i = 0; i < copies; i++) config.record(knownPacks, ProtocolVersion.MINECRAFT_1_21_11);
      return config.finish();
    } finally { packet.release(); input.release(); }
  }
  @Test void compoundKeyOrderIsHarmlessButRegistryIdOrderAndValuesMustMatch() {
    String a=measured(registry((byte)1,false,false)); assertFalse(a.isEmpty());
    assertEquals(a,measured(registry((byte)1,true,false)));
    assertNotEquals(a,measured(registry((byte)1,false,true)));
    assertNotEquals(a,measured(registry((byte)2,false,false)));
  }
  @Test void malformedRegistryCannotAuthorizeReuse() {
    assertEquals("",measured(Unpooled.wrappedBuffer(new byte[]{1,2,3})));
  }
  @Test void duplicateKnownPacksPayloadDoesNotChangeFingerprintButDistinctPayloadDoes() {
    String one = measuredWithKnownPacks("vanilla", 1);
    assertEquals(one, measuredWithKnownPacks("vanilla", 2));
    assertNotEquals(one, measuredWithKnownPacks("vanilla-extra", 1));
  }

  private String fingerprint(byte value) {
    var input = registry(value, false, false);
    var registry = new RegistrySyncPacket();
    try {
      registry.decode(input, ProtocolUtils.Direction.CLIENTBOUND, ProtocolVersion.MINECRAFT_1_21_11);
      var hash = new CanopyConfiguration();
      int before = registry.content().readerIndex();
      hash.record(registry, ProtocolVersion.MINECRAFT_1_21_11);
      assertEquals(before, registry.content().readerIndex(), "measurement must not consume forwarded registry bytes");
      hash.record(new TagsUpdatePacket(Map.of("minecraft:block", Map.of("minecraft:mineable/pickaxe", new int[] {1, 2}))),
          ProtocolVersion.MINECRAFT_1_21_11);
      return hash.finish();
    } finally { registry.release(); input.release(); }
  }
  @Test void tagOrderDoesNotRejectMatchingConfigurationButChangedMembershipDoes() {
    var input = registry((byte) 1, false, false);
    var registry = new RegistrySyncPacket();
    registry.decode(input, ProtocolUtils.Direction.CLIENTBOUND, ProtocolVersion.MINECRAFT_1_21_11);
    try {
      var a = new CanopyConfiguration(); var b = new CanopyConfiguration(); var c = new CanopyConfiguration();
      for (var config : java.util.List.of(a, b, c)) config.record(registry, ProtocolVersion.MINECRAFT_1_21_11);
      a.record(new com.velocitypowered.proxy.protocol.packet.config.TagsUpdatePacket(java.util.Map.of("minecraft:block", java.util.Map.of("minecraft:logs", new int[] {8, 4}))), ProtocolVersion.MINECRAFT_1_21_11);
      b.record(new com.velocitypowered.proxy.protocol.packet.config.TagsUpdatePacket(java.util.Map.of("minecraft:block", java.util.Map.of("minecraft:logs", new int[] {4, 8}))), ProtocolVersion.MINECRAFT_1_21_11);
      c.record(new com.velocitypowered.proxy.protocol.packet.config.TagsUpdatePacket(java.util.Map.of("minecraft:block", java.util.Map.of("minecraft:logs", new int[] {4, 9}))), ProtocolVersion.MINECRAFT_1_21_11);
      String first = a.finish(), reordered = b.finish(), changed = c.finish();
      assertEquals(first, reordered); assertNotEquals(reordered, changed);
    } finally { registry.release(); input.release(); }
  }

  @Test void comparesActualRegistryConfiguration() {
    assertEquals(fingerprint((byte) 1), fingerprint((byte) 1));
    assertNotEquals(fingerprint((byte) 1), fingerprint((byte) 2));
  }
  @Test void incompleteConfigurationCannotAuthorizeReuse() {
    assertEquals("", new CanopyConfiguration().finish());
  }
}
