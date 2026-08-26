package com.folypizza.canopy.halo;

import com.folypizza.canopy.model.TilePosition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tracks tile versions per neighbor shard for lazy-pull halo invalidation.
 *
 * Each shard maintains a map of tile → version for each of its neighbors.
 * When a tile on the local shard is modified, the version is bumped.
 * When a neighbor requests tiles, only those with version > the
 * neighbor's known version are included.
 */
public class TileVersionStore {
    private static final int INFINITE_VERSION = Integer.MAX_VALUE;

    // shard_id -> tile_key -> version
    private final long shardId;
    private final ConcurrentHashMap<Long, Integer> localVersions = new ConcurrentHashMap<>();

    // Track which tiles are adjacent to seams and should notify neighbors
    private final ConcurrentHashMap<Long, Integer> tileVersionHistory = new ConcurrentHashMap<>();

    public TileVersionStore(Long shardId) {
        this.shardId = shardId;
    }

    /**
     * Increment version for a tile (called when a block or tile entity changes).
     */
    public int incrementVersion(TilePosition tile) {
        long tileKey = tileToKey(tile);
        int newVersion = 1 + localVersions.compute(tileKey, (k, v) -> v == null ? 1 : v + 1);
        tileVersionHistory.put(tileKey, newVersion);
        return newVersion;
    }

    /**
     * Get the current version of a tile, or 1 if it doesn't exist.
     */
    public int getVersion(TilePosition tile) {
        long tileKey = tileToKey(tile);
        return localVersions.getOrDefault(tileKey, 1);
    }

    /**
     * Get all tiles that have been updated since the given version.
     * Used when a neighbor queries for updates.
     */
    public Map<TilePosition, Integer> getUpdatesSince(TilePosition tile, int minVersion) {
        // Return every locally-tracked tile whose version is newer than the caller's.
        // The caller narrows to its halo strip via the tile-range filter on the query side.
        Map<TilePosition, Integer> result = new HashMap<>();
        for (var e : localVersions.entrySet()) {
            if (e.getValue() > minVersion) {
                result.put(keyToTile(e.getKey()), e.getValue());
            }
        }
        return result;
    }

    /**
     * Apply updates from a neighbor (received during lazy-pull).
     */
    public void applyUpdates(TilePosition tile, int version) {
        long tileKey = tileToKey(tile);
        int currentVersion = localVersions.getOrDefault(tileKey, 1);
        if (version > currentVersion) {
            // Incoming tile is newer than ours — apply the update
            localVersions.put(tileKey, version);
        }
        // If version <= currentVersion, incoming is stale or current — ignore
    }

    /**
     * Get the delta of all locally-modified tiles since a snapshot.
     * This is what a neighbor receives during a push phase.
     */
    public Map<Long, Integer> getLocalDeltas(int sinceVersion) {
        return localVersions.entrySet().stream()
            .filter(e -> e.getValue() > sinceVersion)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Reset tile versions to the current state of a neighbor's tiles.
     * Called when tiles are first populated into the halo.
     */
    public void resetVersions(TilePosition tile, int version) {
        long tileKey = tileToKey(tile);
        // Don't lower versions — only raise them
        int current = localVersions.getOrDefault(tileKey, 0);
        if (version > current) {
            localVersions.put(tileKey, version);
        }
    }

    /**
     * Get the maximum local tile version (used for health monitoring).
     */
    public int getMaxVersion() {
        return localVersions.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    }

    /**
     * Get all tracked tiles and their versions.
     */
    public Map<Long, Integer> getAllLatestVersions() {
        return new ConcurrentHashMap<>(localVersions);
    }

    /**
     * Get the total number of tracked tiles.
     */
    public int getTrackedCount() {
        return localVersions.size();
    }

    /** Convert tile to a unique key for map storage */
    private static long tileToKey(TilePosition tile) {
        // Combine x and z into a long
        return (((long) tile.x() << 32) | (tile.z() & 0xffffffffL));
    }

    /** Convert a key back to a tile */
    public static TilePosition keyToTile(long tileKey) {
        return new TilePosition(
            (int) (tileKey >>> 32),
            (int) tileKey
        );
    }
}
