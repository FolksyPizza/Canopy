package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;

import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.List;
import org.junit.jupiter.api.Test;

class CanopyChunkViewTest {
  private static final int[] NATIVE_PROTOCOLS = {771, 772, 773, 774, 775, 776, 777};
  private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

  @Test void everyNativeProtocolForwardsInitialChunksAndSuppressesIdenticalChunks() {
    for (int protocol : NATIVE_PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      var view = new CanopyChunkView();
      view.protocol(protocol);
      ByteBuf first = chunk(profile, 2, 3, 0x11);
      ByteBuf same = chunk(profile, 2, 3, 0x11);
      try {
        assertNull(view.onClientbound(first, ALLOC), "initial chunk must be forwarded for protocol " + protocol);
        assertEquals(0, first.readerIndex());
        List<ByteBuf> duplicate = view.onClientbound(same, ALLOC);
        assertNotNull(duplicate, "known chunk must be handled for protocol " + protocol);
        assertTrue(duplicate.isEmpty(), "identical chunk must be suppressed for protocol " + protocol);
        assertEquals(0, same.readerIndex());
        assertTrue(view.stats().contains("1 identical"));
      } finally {
        first.release();
        same.release();
        view.clear();
      }
    }
  }

  @Test void blockUpdatesInvalidateCachedChunksForEveryNativeProtocol() {
    for (int protocol : NATIVE_PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      var view = new CanopyChunkView();
      view.protocol(protocol);
      ByteBuf first = chunk(profile, 2, 3, 0x11);
      ByteBuf blockUpdate = Unpooled.buffer();
      ProtocolUtils.writeVarInt(blockUpdate, profile.clientboundWire(CanopyChunkView.CB_BLOCK_UPDATE));
      blockUpdate.writeLong(blockPosition(33, 64, 49));
      ByteBuf same = chunk(profile, 2, 3, 0x11);
      try {
        assertNull(view.onClientbound(first, ALLOC));
        assertNull(view.onClientbound(blockUpdate, ALLOC));
        assertNull(view.onClientbound(same, ALLOC),
            "chunk changed by a block packet must be sent whole for protocol " + protocol);
        assertTrue(view.stats().contains("0 identical"));
        assertEquals(0, first.readerIndex());
        assertEquals(0, blockUpdate.readerIndex());
        assertEquals(0, same.readerIndex());
      } finally {
        first.release();
        blockUpdate.release();
        same.release();
        view.clear();
      }
    }
  }

  @Test void limboChunkOverlayUsesEachNativeWireIdAndFallsBackToWholeChunks() {
    for (int protocol : NATIVE_PROTOCOLS) {
      var profile = CanopyProtocolProfile.find(protocol);
      var view = new CanopyChunkView();
      view.protocol(protocol);
      view.shadowStarted(chunkX -> true);
      ByteBuf source = chunk(profile, 2, 3, 0x11);
      ByteBuf identical = chunk(profile, 2, 3, 0x11);
      ByteBuf changed = chunk(profile, 2, 3, 0x22);
      List<ByteBuf> replacement = null;
      try {
        assertNull(view.onClientbound(source, ALLOC));
        List<ByteBuf> duplicate = view.onShadow(identical, ALLOC);
        assertNotNull(duplicate);
        assertTrue(duplicate.isEmpty(), "identical limbo chunk must be suppressed for protocol " + protocol);

        replacement = view.onShadow(changed, ALLOC);
        assertNotNull(replacement);
        assertEquals(1, replacement.size(), "changed limbo chunk must be retained for protocol " + protocol);
        ByteBuf packet = replacement.getFirst();
        assertEquals(profile.clientboundWire(CanopyChunkView.CB_LEVEL_CHUNK_WITH_LIGHT),
            ProtocolUtils.readVarInt(packet));
        assertEquals(2, packet.readInt());
        assertEquals(3, packet.readInt());
        assertEquals(0x22, packet.readUnsignedByte());
      } finally {
        if (replacement != null) replacement.forEach(ByteBuf::release);
        source.release();
        identical.release();
        changed.release();
        view.clear();
      }
    }
  }

  private static ByteBuf chunk(CanopyProtocolProfile profile, int x, int z, int marker) {
    ByteBuf packet = Unpooled.buffer();
    ProtocolUtils.writeVarInt(packet, profile.clientboundWire(CanopyChunkView.CB_LEVEL_CHUNK_WITH_LIGHT));
    packet.writeInt(x).writeInt(z).writeByte(marker);
    return packet;
  }

  private static long blockPosition(int x, int y, int z) {
    return ((long) x & 0x3ffffffL) << 38 | ((long) z & 0x3ffffffL) << 12 | y & 0xfffL;
  }
}
