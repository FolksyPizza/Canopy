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
