package com.folypizza.canopy.transfer;

import com.folypizza.canopy.proto.TileData;
import com.folypizza.canopy.proto.SectionDelta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Tile delta serializer — compact binary format for inter-shard tile updates.
 *
 * Each tile is 16x16 chunks (256 blocks). Sections are stored compactly
 * in each tile: version varint, section Y offset (int8), block count,
 * then array of block state indices (int32 per slab).
 */
public class TileDeltaSerializer {
    private static final Logger log = LoggerFactory.getLogger(TileDeltaSerializer.class);
    private static final int BLOCKS_PER_SLAB = 16;
    private static final int SLAB_BYTE_SIZE = BLOCKS_PER_SLAB * 4;
    private static final int SECTIONS_PER_TILE = 24;

    public record SerializedTile(int sectionY, int slabCount, List<int[]> slabs, int estimatedSize) {}

    /**
     * Serialize a single protobuf tile entry to a compact byte array.
     * Format:
     *   [tilePos: int32 x] [tilePos: int32 z] [version: varint]
     *   [sectionCount: varint]
     *   For each section: [sectionY: int8] [slabCount: varint] [slabs...]
     */
    public byte[] serializeTile(TileData tile) {
        int estimatedSize = estimateSerializedSize(tile) + 8;
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        buffer.putInt(tile.getPosition().getX());
        buffer.putInt(tile.getPosition().getZ());

        // Write version as fixed size for simplicity
        buffer.putInt(tile.getVersion());

        List<SectionDelta> sections = tile.getSectionsList();
        buffer.putShort((short) sections.size());

        for (SectionDelta section : sections) {
            buffer.putInt(section.getSectionY());
            int[] blocks = section.getBlocksList().stream().mapToInt(Integer::intValue).toArray();
            buffer.putFloat((float) blocks.length / BLOCKS_PER_SLAB);

            for (int slab : blocks) {
                buffer.putInt(slab);
            }
        }

        return buffer.array();
    }

    /**
     * Deserialize a compact byte array back into a protobuf TileData entry.
     */
    public SerializedTile deserializeTile(ByteBuffer buffer) {
        int tileX = buffer.getInt();
        int tileZ = buffer.getInt();
        int version = buffer.getInt();
        short sectionCount = buffer.getShort();

        List<int[]> slabs = new ArrayList<>();
        for (int i = 0; i < sectionCount; i++) {
            int sectionY = buffer.getInt();
            float slabFrac = buffer.getFloat();
            int slabCount = (int) (slabFrac * BLOCKS_PER_SLAB);
            int[] slabData = new int[slabCount];
            for (int j = 0; j < slabCount; j++) {
                slabData[j] = buffer.getInt();
            }
            slabs.add(slabData);
        }

        int estimatedSize = buffer.position();
        return new SerializedTile(0, slabs.size(), slabs, estimatedSize);
    }

    /**
     * Estimate the byte size of a tile before serialization.
     */
    public int estimateSerializedSize(TileData tile) {
        int size = 8 + 4; // tile pos (2x int32) + version
        List<SectionDelta> sections = tile.getSectionsList();
        size += 2; // section count
        for (SectionDelta s : sections) {
            size += 4 + 4 + s.getBlocksList().size() * 4;
        }
        return size;
    }

    /**
     * Serializes a full tile to a list of section delta records.
     */
    public List<SerializedTile> serializeFullTile(TileData tile) {
        List<SerializedTile> result = new ArrayList<>();
        for (SectionDelta section : tile.getSectionsList()) {
            result.add(new SerializedTile(
                section.getSectionY(),
                section.getBlocksCount(),
                List.of(section.getBlocksList().stream().mapToInt(Integer::intValue).toArray()),
                estimateSerializedSize(tile)
            ));
        }
        return result;
    }

    /**
     * Serialize delta sections for dirty tracking (only changed sections).
     */
    public byte[] serializeSectionDeltas(List<int[]> sectionDeltas) {
        int totalSize = 4; // count
        for (int[] delta : sectionDeltas) {
            totalSize += 4 + 4 + delta.length * 4; // sectionY + count + blocks
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(sectionDeltas.size());
        for (int[] delta : sectionDeltas) {
            buffer.putInt(delta.length);
            for (int block : delta) {
                buffer.putInt(block);
            }
        }
        return buffer.array();
    }

    /**
     * Deserialize section deltas from a dirty-tracking batch.
     */
    public List<int[]> deserializeSectionDeltas(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int count = buffer.getInt();
        List<int[]> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int blockCount = buffer.getInt();
            int[] delta = new int[blockCount];
            for (int j = 0; j < blockCount; j++) {
                delta[j] = buffer.getInt();
            }
            result.add(delta);
        }
        return result;
    }
}
