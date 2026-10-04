package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class CanopyKnownPacksTest {
  @Test void preservesEmptySelectionInsteadOfClaimingAdvertisedPacks() {
    byte[] none = {0};
    assertArrayEquals(none, CanopyKnownPacks.encode(CanopyKnownPacks.decode(none, ProtocolVersion.MINECRAFT_1_21_11), ProtocolVersion.MINECRAFT_1_21_11));
  }
  @Test void storesAnIndependentCopyOfSelectedPackNamesAndVersions() {
    var buffer = Unpooled.buffer();
    try {
      ProtocolUtils.writeVarInt(buffer,1); ProtocolUtils.writeString(buffer,"minecraft");
      ProtocolUtils.writeString(buffer,"core"); ProtocolUtils.writeString(buffer,"1.21.11");
      byte[] original = new byte[buffer.readableBytes()]; buffer.getBytes(0,original);
      var packet = CanopyKnownPacks.decode(original, ProtocolVersion.MINECRAFT_1_21_11);
      byte[] saved = CanopyKnownPacks.encode(packet, ProtocolVersion.MINECRAFT_1_21_11);
      original[0]=0;
      assertEquals(1,saved[0]);
      assertArrayEquals(saved,CanopyKnownPacks.encode(CanopyKnownPacks.decode(saved, ProtocolVersion.MINECRAFT_1_21_11), ProtocolVersion.MINECRAFT_1_21_11));
    } finally { buffer.release(); }
  }
}
