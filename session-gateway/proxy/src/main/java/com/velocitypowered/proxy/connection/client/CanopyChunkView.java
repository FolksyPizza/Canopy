/*
 * Copyright (C) 2018-2025 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The client's view of the world, owned by the gateway.
 *
 * <p>The gateway keeps the exact bytes of every chunk the client holds. When a backend sends a chunk the client already
 * has, only the difference reaches the client: nothing if the chunk is identical, the changed blocks (and light) if
 * only block states differ, and the whole chunk only when the difference cannot be expressed as block changes (biomes,
 * block entities, a different section layout). A backend that becomes authoritative for the player therefore never
 * rebuilds the client's world; anything that differs is layered on top of what the client already shows.
 *
 * <p>A chunk a backend changes after sending it (block, block entity or light updates) no longer matches the stored
 * copy, so it is marked dirty and the next version is sent whole: deltas are only ever computed against a copy that is
 * known to match the client.
 *
 * <p>Protocol 774 (1.21.11): chunk data without long-array length prefixes, overworld (24 sections from y=-64) and
 * nether/end (16 sections from y=0) heights.
 */
public final class CanopyChunkView {

  private CanopyProtocolProfile profile = CanopyProtocolProfile.find(774);

  public synchronized void protocol(int protocol) {
    CanopyProtocolProfile selected = CanopyProtocolProfile.find(protocol);
    if (selected == null) { throw new IllegalArgumentException("Unsupported Canopy chunk protocol"); }
    if (selected != profile) { clear(); }
    profile = selected;
  }

  static final int CB_BLOCK_ENTITY_DATA = 0x06;
  static final int CB_BLOCK_UPDATE = 0x08;
  static final int CB_FORGET_LEVEL_CHUNK = 0x25;
  static final int CB_LEVEL_CHUNK_WITH_LIGHT = 0x2c;
  static final int CB_LIGHT_UPDATE = 0x2f;
  static final int CB_RESPAWN = 0x50;
  static final int CB_SECTION_BLOCKS_UPDATE = 0x52;
  /** Beyond this many changed blocks a whole chunk is cheaper than section updates. */
  private static final int MAX_DELTA_BLOCKS = 8192;

  private static final class Held {
    final byte[] payload;       // the packet after its id
    boolean blocksDirty;        // changed by the backend since it was sent
    boolean lightDirty;

    Held(byte[] payload) {
      this.payload = payload;
    }
  }

  /**
   * Memory bound per player (-Dcanopy.chunkView.maxBytes, default 16 MiB). The least recently sent chunks are evicted
   * first; an evicted chunk is simply sent whole the next time, so the bound costs smoothness, never correctness.
   */
  private static final long MAX_BYTES = Long.getLong("canopy.chunkView.maxBytes", 16L << 20);

  private final java.util.LinkedHashMap<Long, Held> held = new java.util.LinkedHashMap<>(256, 0.75f, true);
  private long heldBytes;

  // Shadow session: the destination is authoritative for the chunks it owns. Its version of each is kept here, and
  // the source's (stale) copies of those chunks never reach the client while the shadow is open.
  private java.util.function.IntPredicate shadowOwns;
  private final java.util.HashMap<Long, byte[]> overlay = new java.util.HashMap<>();
  private long chunksDropped;      // byte-identical
  private long chunksEquivalent;   // same blocks and light (other bytes, such as heightmaps, differ)
  private long chunksLightOnly;
  private long chunksDelta;        // sent as block changes
  private long chunksReplaced;
  private String lastReplaced = "";

  /**
   * Handles a clientbound packet. Returns null to forward it unchanged, or the packets to send instead (possibly
   * none).
   */
  public synchronized List<ByteBuf> onClientbound(ByteBuf buf, ByteBufAllocator alloc) {
    int start = buf.readerIndex();
    try {
      int id = profile.clientbound(ProtocolUtils.readVarInt(buf));
      switch (id) {
        case CB_LEVEL_CHUNK_WITH_LIGHT: {
          if (shadowOwns != null) {
            int x = buf.getInt(buf.readerIndex());
            int z = buf.getInt(buf.readerIndex() + 4);
            byte[] authoritative = shadowOwns.test(x) ? overlay.get(key(x, z)) : null;
            if (authoritative != null) {
              return orWhole(deliver(key(x, z), x, z, authoritative, alloc), authoritative, alloc);
            }
          }
          return onChunk(buf, alloc);
        }
        case CB_FORGET_LEVEL_CHUNK: {
          int z = buf.readInt();
          int x = buf.readInt();
          forget(key(x, z));
          return null;
        }
        case CB_BLOCK_UPDATE:
        case CB_BLOCK_ENTITY_DATA: {
          long pos = buf.readLong();
          int x = (int) (pos >> 38);
          int z = (int) (pos << 26 >> 38);
          if (shadowOwns != null && shadowOwns.test(x >> 4)) {
            return List.of();   // the source's copy of the destination's side
          }
          markBlocksDirty(x >> 4, z >> 4);
          return null;
        }
        case CB_SECTION_BLOCKS_UPDATE: {
          long section = buf.readLong();
          if (shadowOwns != null && shadowOwns.test((int) (section >> 42))) {
            return List.of();
          }
          markBlocksDirty((int) (section >> 42), (int) (section << 22 >> 42));
          return null;
        }
        case CB_LIGHT_UPDATE: {
          int x = ProtocolUtils.readVarInt(buf);
          int z = ProtocolUtils.readVarInt(buf);
          Held h = held.get(key(x, z));
          if (h != null) {
            h.lightDirty = true;
          }
          return null;
        }
        case CB_RESPAWN:
          clear();   // the client drops its chunks on a respawn
          return null;
        default:
          return null;
      }
    } catch (Exception ex) {
      return null;
    } finally {
      buf.readerIndex(start);
    }
  }

  /** A computed packet was abandoned before delivery: cached bytes may no longer match the client. */
  public synchronized void invalidateAll() {
    held.values().forEach(chunk -> { chunk.blocksDirty = true; chunk.lightDirty = true; });
    overlay.clear();
  }

  public synchronized void clear() {
    held.clear();
    heldBytes = 0;
    overlay.clear();
  }

  public synchronized void shadowStarted(java.util.function.IntPredicate destinationOwnsChunkX) {
    shadowOwns = destinationOwnsChunkX;
    overlay.clear();
  }

  public synchronized void shadowEnded() {
    shadowOwns = null;
    overlay.clear();
  }

  /**
   * A packet from the shadow session. The destination's chunks and block changes on its own side become deltas for
   * the client (only for chunks the client holds); everything else is dropped.
   */
  public synchronized List<ByteBuf> onShadow(ByteBuf buf, ByteBufAllocator alloc) {
    if (shadowOwns == null) {
      return List.of();
    }
    int start = buf.readerIndex();
    try {
      int id = profile.clientbound(ProtocolUtils.readVarInt(buf));
      switch (id) {
        case CB_LEVEL_CHUNK_WITH_LIGHT: {
          int x = buf.getInt(buf.readerIndex());
          int z = buf.getInt(buf.readerIndex() + 4);
          if (!shadowOwns.test(x)) {
            return List.of();
          }
          byte[] payload = new byte[buf.readableBytes()];
          buf.getBytes(buf.readerIndex(), payload);
          overlay.put(key(x, z), payload);
          if (!held.containsKey(key(x, z))) {
            return List.of();   // not in the client's view yet; used when the source sends it
          }
          return orWhole(deliver(key(x, z), x, z, payload, alloc), payload, alloc);
        }
        case CB_BLOCK_UPDATE:
        case CB_BLOCK_ENTITY_DATA:
        case CB_SECTION_BLOCKS_UPDATE: {
          long pos = buf.readLong();
          int cx = id == CB_SECTION_BLOCKS_UPDATE ? (int) (pos >> 42) : (int) (pos >> 38) >> 4;
          int cz = id == CB_SECTION_BLOCKS_UPDATE ? (int) (pos << 22 >> 42) : (int) (pos << 26 >> 38) >> 4;
          if (!shadowOwns.test(cx) || !held.containsKey(key(cx, cz))) {
            return List.of();
          }
          // A live change on the destination's side: the client sees it now. The stored copies no longer match.
          overlay.remove(key(cx, cz));
          markBlocksDirty(cx, cz);
          buf.readerIndex(start);
          return List.of(buf.retainedDuplicate());
        }
        case CB_FORGET_LEVEL_CHUNK: {
          int z = buf.readInt();
          int x = buf.readInt();
          overlay.remove(key(x, z));
          return List.of();
        }
        default:
          return List.of();
      }
    } catch (Exception ex) {
      return List.of();
    } finally {
      buf.readerIndex(start);
    }
  }

  /** {@code result} from {@link #deliver}, or the whole chunk when it has to be sent in full. */
  private List<ByteBuf> orWhole(List<ByteBuf> result, byte[] payload, ByteBufAllocator alloc) {
    if (result != null) {
      return result;
    }
    ByteBuf whole = alloc.buffer(payload.length + 2);
    ProtocolUtils.writeVarInt(whole, profile.clientboundWire(CB_LEVEL_CHUNK_WITH_LIGHT));
    whole.writeBytes(payload);
    return List.of(whole);
  }

  private void forget(long k) {
    Held h = held.remove(k);
    if (h != null) {
      heldBytes -= h.payload.length;
    }
  }

  private Held remember(long k, byte[] payload) {
    Held old = held.remove(k);
    if (old != null) {
      heldBytes -= old.payload.length;
    }
    held.put(k, new Held(payload));
    heldBytes += payload.length;
    var eldest = held.entrySet().iterator();
    while (heldBytes > MAX_BYTES && eldest.hasNext()) {
      var e = eldest.next();
      if (e.getKey() == k) {
        continue;
      }
      heldBytes -= e.getValue().payload.length;
      eldest.remove();
    }
    return old;
  }

  public synchronized String stats() {
    return chunksDropped + " identical, " + chunksEquivalent + " equivalent, " + chunksLightOnly + " light only, "
        + chunksDelta + " as block deltas, " + chunksReplaced + " replaced"
        + (lastReplaced.isEmpty() ? "" : " (last: " + lastReplaced + ")");
  }

  private void markBlocksDirty(int cx, int cz) {
    Held h = held.get(key(cx, cz));
    if (h != null) {
      h.blocksDirty = true;
    }
  }

  private List<ByteBuf> onChunk(ByteBuf buf, ByteBufAllocator alloc) {
    byte[] payload = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), payload);
    int x = buf.readInt();
    int z = buf.readInt();
    return deliver(key(x, z), x, z, payload, alloc);
  }

  /**
   * The client is to hold {@code payload} for chunk (x, z). Returns what to send: nothing, deltas, or null for the
   * whole chunk.
   */
  private List<ByteBuf> deliver(long k, int x, int z, byte[] payload, ByteBufAllocator alloc) {
    Held old = remember(k, payload);
    if (old == null || old.blocksDirty) {
      if (old != null) {
        chunksReplaced++;
        lastReplaced = "(" + x + "," + z + ") changed by the backend after it was sent";
      }
      return null;
    }
    if (!old.lightDirty && Arrays.equals(old.payload, payload)) {
      chunksDropped++;
      return List.of();
    }
    // Newer chunk layouts retain their complete wire payload until their delta codecs are verified.
    if (profile.protocol() > 774) {
      chunksReplaced++;
      return null;
    }
    ChunkParts before = ChunkParts.parse(old.payload);
    ChunkParts after = ChunkParts.parse(payload);
    if (before == null || after == null || !before.sameShape(after)) {
      chunksReplaced++;
      lastReplaced = "(" + x + "," + z + ") " + (before == null || after == null ? "unparsed"
          : before.sections.size() != after.sections.size() ? "section count"
          : !Arrays.equals(before.biomes, after.biomes) ? "biomes" : "block entities");
      return null;
    }
    List<ByteBuf> out = new ArrayList<>();
    int changed = 0;
    boolean blocks = false;
    int minSection = after.sections.size() == 24 ? -4 : 0;
    for (int s = 0; s < after.sections.size(); s++) {
      Section a = before.sections.get(s);
      Section b = after.sections.get(s);
      if (Arrays.equals(a.blockBytes, b.blockBytes)) {
        continue;
      }
      int[] from = a.states();
      int[] to = b.states();
      if (from == null || to == null) {
        release(out);
        chunksReplaced++;
        return null;
      }
      List<Long> records = new ArrayList<>();
      for (int i = 0; i < 4096; i++) {
        if (from[i] != to[i]) {
          // Section-local index is (y << 8) | (z << 4) | x; the record packs (x << 8) | (z << 4) | y.
          int lx = i & 15;
          int lz = (i >> 4) & 15;
          int ly = i >> 8;
          records.add(((long) to[i] << 12) | (lx << 8) | (lz << 4) | ly);
        }
      }
      changed += records.size();
      if (changed > MAX_DELTA_BLOCKS) {
        release(out);
        chunksReplaced++;
        return null;
      }
      ByteBuf update = alloc.buffer();
      ProtocolUtils.writeVarInt(update, profile.clientboundWire(CB_SECTION_BLOCKS_UPDATE));
      update.writeLong(((long) (x & 0x3FFFFF) << 42) | ((long) (z & 0x3FFFFF) << 20)
          | ((minSection + s) & 0xFFFFF));
      ProtocolUtils.writeVarInt(update, records.size());
      for (long r : records) {
        writeVarLong(update, r);
      }
      out.add(update);
      blocks = true;
    }
    if (old.lightDirty || !Arrays.equals(before.light, after.light)) {
      ByteBuf light = alloc.buffer();
      ProtocolUtils.writeVarInt(light, profile.clientboundWire(CB_LIGHT_UPDATE));
      ProtocolUtils.writeVarInt(light, x);
      ProtocolUtils.writeVarInt(light, z);
      light.writeBytes(after.light);
      out.add(light);
    }
    if (blocks) {
      chunksDelta++;
    } else if (out.isEmpty()) {
      chunksEquivalent++;
    } else {
      chunksLightOnly++;
    }
    return out;
  }

  private static void release(List<ByteBuf> bufs) {
    bufs.forEach(ByteBuf::release);
    bufs.clear();
  }

  private static long key(int x, int z) {
    return ((long) x << 32) | (z & 0xffffffffL);
  }

  private static void writeVarLong(ByteBuf buf, long value) {
    while ((value & ~0x7FL) != 0) {
      buf.writeByte((int) (value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buf.writeByte((int) value);
  }

  /** The parts of a chunk packet that decide how a difference can be sent. */
  private static final class ChunkParts {
    final byte[] blockEntities;
    final List<Section> sections = new ArrayList<>();
    byte[] biomes;
    byte[] light;

    private ChunkParts(byte[] blockEntities) {
      this.blockEntities = blockEntities;
    }

    /** Block changes (and a light update) can express the difference: same layout, biomes and block entities. */
    boolean sameShape(ChunkParts other) {
      return sections.size() == other.sections.size()
          && (sections.size() == 24 || sections.size() == 16)
          && Arrays.equals(biomes, other.biomes)
          && Arrays.equals(blockEntities, other.blockEntities);
    }

    static ChunkParts parse(byte[] payload) {
      try {
        ByteBuf buf = Unpooled.wrappedBuffer(payload);
        buf.skipBytes(8);   // x, z
        int maps = ProtocolUtils.readVarInt(buf);
        for (int i = 0; i < maps; i++) {
          ProtocolUtils.readVarInt(buf);
          buf.skipBytes(8 * ProtocolUtils.readVarInt(buf));
        }
        int dataLength = ProtocolUtils.readVarInt(buf);
        ByteBuf data = buf.readSlice(dataLength);
        int blockEntitiesStart = buf.readerIndex();
        int count = ProtocolUtils.readVarInt(buf);
        for (int i = 0; i < count; i++) {
          buf.skipBytes(1 + 2);
          ProtocolUtils.readVarInt(buf);
          Nbt.skipAnonymous(buf);
        }
        // Block entities are compared as bytes (heightmaps are recomputed by the client on block changes).
        byte[] blockEntities = new byte[buf.readerIndex() - blockEntitiesStart];
        buf.getBytes(blockEntitiesStart, blockEntities);
        ChunkParts parts = new ChunkParts(blockEntities);
        parts.light = new byte[buf.readableBytes()];
        buf.readBytes(parts.light);
        ByteArrayBuilder biomes = new ByteArrayBuilder();
        while (data.isReadable()) {
          int sectionStart = data.readerIndex();
          data.skipBytes(2);   // non-air block count
          PalettedContainer.skip(data, 4096, 8);
          byte[] blockBytes = new byte[data.readerIndex() - sectionStart];
          data.getBytes(sectionStart, blockBytes);
          int biomeStart = data.readerIndex();
          PalettedContainer.skip(data, 64, 3);
          biomes.append(data, biomeStart, data.readerIndex() - biomeStart);
          parts.sections.add(new Section(blockBytes));
        }
        parts.biomes = biomes.toArray();
        return parts;
      } catch (Exception ex) {
        return null;
      }
    }
  }

  /** One chunk section's block data: the non-air count and the block-state container. */
  private static final class Section {
    final byte[] blockBytes;

    Section(byte[] blockBytes) {
      this.blockBytes = blockBytes;
    }

    int[] states() {
      try {
        ByteBuf buf = Unpooled.wrappedBuffer(blockBytes);
        buf.skipBytes(2);
        return PalettedContainer.read(buf, 4096, 8);
      } catch (Exception ex) {
        return null;
      }
    }
  }

  /** Paletted containers as sent since 1.21.5 (no long-array length prefix). */
  static final class PalettedContainer {
    static int[] read(ByteBuf buf, int entries, int maxIndirectBits) {
      int bits = buf.readUnsignedByte();
      int[] out = new int[entries];
      if (bits == 0) {
        Arrays.fill(out, ProtocolUtils.readVarInt(buf));
        return out;
      }
      int[] palette = null;
      if (bits <= maxIndirectBits) {
        palette = new int[ProtocolUtils.readVarInt(buf)];
        for (int i = 0; i < palette.length; i++) {
          palette[i] = ProtocolUtils.readVarInt(buf);
        }
      }
      int perLong = 64 / bits;
      long mask = (1L << bits) - 1;
      int longs = (entries + perLong - 1) / perLong;
      int index = 0;
      for (int l = 0; l < longs; l++) {
        long word = buf.readLong();
        for (int j = 0; j < perLong && index < entries; j++, index++) {
          int value = (int) ((word >>> (j * bits)) & mask);
          out[index] = palette == null ? value : palette[value];
        }
      }
      return out;
    }

    static void skip(ByteBuf buf, int entries, int maxIndirectBits) {
      int bits = buf.readUnsignedByte();
      if (bits == 0) {
        ProtocolUtils.readVarInt(buf);
        return;
      }
      if (bits <= maxIndirectBits) {
        int size = ProtocolUtils.readVarInt(buf);
        for (int i = 0; i < size; i++) {
          ProtocolUtils.readVarInt(buf);
        }
      }
      int perLong = 64 / bits;
      buf.skipBytes(8 * ((entries + perLong - 1) / perLong));
    }
  }

  /** Skips network NBT (anonymous root, as sent since 1.20.2). */
  static final class Nbt {
    static void skipAnonymous(ByteBuf buf) {
      int type = buf.readUnsignedByte();
      if (type != 0) {
        skipPayload(buf, type);
      }
    }

    private static void skipPayload(ByteBuf buf, int type) {
      switch (type) {
        case 1 -> buf.skipBytes(1);
        case 2 -> buf.skipBytes(2);
        case 3, 5 -> buf.skipBytes(4);
        case 4, 6 -> buf.skipBytes(8);
        case 7 -> buf.skipBytes(buf.readInt());
        case 8 -> buf.skipBytes(buf.readUnsignedShort());
        case 9 -> {
          int inner = buf.readUnsignedByte();
          int length = buf.readInt();
          for (int i = 0; i < length; i++) {
            skipPayload(buf, inner);
          }
        }
        case 10 -> {
          int inner;
          while ((inner = buf.readUnsignedByte()) != 0) {
            buf.skipBytes(buf.readUnsignedShort());
            skipPayload(buf, inner);
          }
        }
        case 11 -> buf.skipBytes(4 * buf.readInt());
        case 12 -> buf.skipBytes(8 * buf.readInt());
        default -> throw new IllegalStateException("NBT type " + type);
      }
    }
  }

  private static final class ByteArrayBuilder {
    private final ByteBuf buf = Unpooled.buffer();

    void append(ByteBuf from, int index, int length) {
      buf.writeBytes(from, index, length);
    }

    byte[] toArray() {
      byte[] out = new byte[buf.readableBytes()];
      buf.getBytes(0, out);
      buf.release();
      return out;
    }
  }
}
