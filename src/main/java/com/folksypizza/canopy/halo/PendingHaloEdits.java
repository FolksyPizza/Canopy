package com.folksypizza.canopy.halo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded, coalescing buffer for peer block edits whose destination chunks are not loaded yet.
 *
 * The latest state for each block is enough to reproduce the visible halo; retaining intermediate
 * fluid updates would waste memory and can make a returning shard replay a large event backlog.
 */
public final class PendingHaloEdits {
    public record Edit(long sequence, int x, int y, int z, String data) {
        public Edit {
            Objects.requireNonNull(data, "data");
        }
    }

    private record BlockPosition(int x, int y, int z) {}

    private final int capacity;
    private final Map<Long, LinkedHashMap<BlockPosition, Edit>> byChunk = new LinkedHashMap<>();
    private int size;

    public PendingHaloEdits(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
    }

    /**
     * Adds an edit, replacing any older pending state for the same block. Returns false only when
     * the buffer is full and the edit introduces another unique block position.
     */
    public synchronized boolean offer(Edit edit) {
        long chunk = chunkKey(edit.x() >> 4, edit.z() >> 4);
        LinkedHashMap<BlockPosition, Edit> edits = byChunk.computeIfAbsent(chunk, ignored -> new LinkedHashMap<>());
        BlockPosition position = new BlockPosition(edit.x(), edit.y(), edit.z());
        Edit previous = edits.get(position);
        if (previous != null) {
            if (edit.sequence() >= previous.sequence()) edits.put(position, edit);
            return true;
        }
        if (size >= capacity) {
            if (edits.isEmpty()) byChunk.remove(chunk);
            return false;
        }
        edits.put(position, edit);
        size++;
        return true;
    }

    public synchronized boolean hasChunk(int chunkX, int chunkZ) {
        return byChunk.containsKey(chunkKey(chunkX, chunkZ));
    }

    /** Removes and returns the currently coalesced edits for a chunk. */
    public synchronized List<Edit> drain(int chunkX, int chunkZ) {
        LinkedHashMap<BlockPosition, Edit> edits = byChunk.remove(chunkKey(chunkX, chunkZ));
        if (edits == null || edits.isEmpty()) return List.of();
        size -= edits.size();
        return List.copyOf(edits.values());
    }

    public synchronized int size() {
        return size;
    }

    public static long chunkKey(int chunkX, int chunkZ) {
        return ((long) chunkX << 32) | (chunkZ & 0xffffffffL);
    }
}
