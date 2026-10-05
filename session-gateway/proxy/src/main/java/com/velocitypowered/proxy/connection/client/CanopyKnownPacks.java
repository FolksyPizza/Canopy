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

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.packet.config.KnownPacksPacket;
import io.netty.buffer.Unpooled;

/** A private copy of the client's selected packs, never the backend's advertised pack list. */
public final class CanopyKnownPacks {
  private CanopyKnownPacks() { }
  public static byte[] encode(KnownPacksPacket packet, ProtocolVersion version) {
    var buffer = Unpooled.buffer();
    try {
      packet.encode(buffer, ProtocolUtils.Direction.SERVERBOUND, version);
      byte[] bytes = new byte[buffer.readableBytes()]; buffer.readBytes(bytes); return bytes;
    } finally { buffer.release(); }
  }
  public static KnownPacksPacket decode(byte[] bytes, ProtocolVersion version) {
    var buffer = Unpooled.wrappedBuffer(bytes);
    try {
      var packet = new KnownPacksPacket(); packet.decode(buffer, ProtocolUtils.Direction.SERVERBOUND, version); return packet;
    } finally { buffer.release(); }
  }
}
