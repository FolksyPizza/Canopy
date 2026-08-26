package com.folypizza.canopy.halo;

import com.folypizza.canopy.model.TilePosition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Tile update tracking for halo synchronization.
 * 
 * When a tile is modified on a shard, it broadcasts updates
 * to all neighboring shards that have that tile in their halo.
 * 
 * In lazy-pull mode: updates are pushed initially, then the
 * neighbor pulls deltas on demand via TileVersionStore.getUpdatesSince().
 */
public class TileUpdateTracker {
    private static final int DEFAULT_MAX_AGE_MS = 5000;

    // tile_key -> update_time_ms
    private final ConcurrentHashMap<Long, Long> updateTimestamps = new ConcurrentHashMap<>();
    
    // tile_key -> last_version (from local shard)
    private final ConcurrentHashMap<Long, Integer> latestVersions = new ConcurrentHashMap<>();
    
    // Neighbor tile -> tile store it should receive updates to
    private final ConcurrentHashMap<Long, TileVersionStore> neighborStores = new ConcurrentHashMap<>();
    
    private final long shardId;
    private final long maxAgeMs;

    public TileUpdateTracker(long shardId) {
        this(shardId, DEFAULT_MAX_AGE_MS);
    }

    public TileUpdateTracker(long shardId, long maxAgeMs) {
        this.shardId = shardId;
        this.maxAgeMs = maxAgeMs;
    }

    /**
     * Record a tile update (called when a block changes on this shard).
     * Returns the new version number.
     */
    public int recordUpdate(TilePosition tile) {
        long tileKey = tileKey(tile);
        int newVersion = 1 + latestVersions.compute(tileKey, (k, v) -> v == null ? 1 : v + 1);
        updateTimestamps.put(tileKey, System.nanoTime());
        notifyNeighbors(tile, newVersion);
        return newVersion;
    }

    /**
     * Get all tiles that have been updated since nanoTime.
     * Used when a neighbor queries for updates.
     */
    public List<TileUpdate> getUpdatesSince(TilePosition tile, long sinceNanoTime) {
        return updateTimestamps.entrySet().stream()
            .filter(entry -> entry.getValue() > sinceNanoTime)
            .map(entry -> {
                TilePosition tilePos = keyToTile(entry.getKey());
                int version = latestVersions.getOrDefault(entry.getKey(), 1);
                return new TileUpdate(tilePos, version, entry.getValue());
            })
            .collect(Collectors.toList());
    }

    /**
     * Get all recently updated tiles (last maxAgeMs).
     * Used for initial halo population.
     */
    public List<TileUpdate> getRecentUpdates(long tileRadius) {
        long cutoff = System.nanoTime() - maxAgeMs * 1_000_000; // convert to nanos
        return updateTimestamps.entrySet().stream()
            .filter(entry -> entry.getValue() > cutoff)
            .map(entry -> {
                TilePosition tp = keyToTile(entry.getKey());
                int version = latestVersions.getOrDefault(entry.getKey(), 1);
                return new TileUpdate(tp, version, entry.getValue());
            })
            .collect(Collectors.toList());
    }

    /**
     * Register a neighbor's tile store for update forwarding.
     */
    public void registerNeighbor(long neighborShardId, TileVersionStore store) {
        neighborStores.put(neighborShardId, store);
    }

    /**
     * Unregister a neighbor's tile store.
     */
    public void unregisterNeighbor(long neighborShardId) {
        neighborStores.remove(neighborShardId);
    }

    /**
     * Clean up stale update entries (garbage collection).
     */
    public void cleanupStaleUpdates() {
        long cutoff = System.nanoTime() - maxAgeMs * 1_000_000;
        updateTimestamps.entrySet().removeIf(entry -> entry.getValue() < cutoff);
    }

    public Map<Long, Integer> getAllLatestVersions() {
        return new ConcurrentHashMap<>(latestVersions);
    }

    public int getTrackedTileCount() {
        return latestVersions.size();
    }

    /** @deprecated use recordUpdate() instead */
    public void updateTileVersion(TilePosition tile, long shardId) {
        recordUpdate(tile);
    }

    public void onChunkEvent(int chunkX, int chunkZ, boolean loaded) {
        // Stub — chunk load tracking handled at a higher level
    }

    /** Batch-mark tiles as updated for peer push. Stub — batching handled elsewhere. */
    public void markUpdatedTiles(List<Map.Entry<Long, com.folypizza.canopy.proto.TileVersion>> updates) {
        // Stub — batch push handled at a higher level
    }

    private void notifyNeighbors(TilePosition tile, int version) {
        for (var entry : neighborStores.entrySet()) {
            long neighborId = entry.getKey();
            TileVersionStore neighborStore = entry.getValue();
            if (neighborStore != null) {
                neighborStore.applyUpdates(tile, version);
            }
        }
    }

    private static long tileKey(TilePosition tile) {
        return (((long) tile.x() << 32) | (tile.z() & 0xffffffffL));
    }

    private static TilePosition keyToTile(long tileKey) {
        return new TilePosition(
            (int) (tileKey >>> 32),
            (int) tileKey
        );
    }

    /**
     * Immutable tile update record.
     */
    public record TileUpdate(TilePosition tile, int version, long timestampNs) {
        public boolean isStale(long maxAgeNs) {
            return timestampNs < (System.nanoTime() - maxAgeNs);
        }
    }
}
