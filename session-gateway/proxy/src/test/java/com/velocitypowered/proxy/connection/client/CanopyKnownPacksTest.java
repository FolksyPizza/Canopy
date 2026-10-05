/*
 * Copyright (C) 2026 Canopy Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */
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
