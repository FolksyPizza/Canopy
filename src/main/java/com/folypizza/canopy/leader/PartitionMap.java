package com.folypizza.canopy.leader;

import com.folypizza.canopy.model.TilePosition;
import com.folypizza.canopy.model.ChunkPosition;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Manages which shard owns a given tile or region of the world.
 *
 * The partition map is the single source of truth for:
 * - Which chunks belong to which shard
 * - Where seams are located
 * - Which tiles need halo updates
 *
 * Version-gating ensures that stale controllers cannot apply
 * conflicting partition changes.
 */
public class PartitionMap {
    private static final int TILE_SIZE = 256; // blocks per tile in each dimension

    private final AtomicLong version = new AtomicLong(0);
    private final Object lock = new Object();
    private volatile PartitionState state;

    /**
     * Immutable partition state.
     * Each shard has a list of tile ranges it owns.
     */
    public record PartitionState(
        long version,
        List<ShardPartition> shards,
        List<SeamBoundary> seams
    ) {
        /**
         * Find which shard owns a given tile position.
         */
        public long findShardForTile(TilePosition tile) {
            for (var shard : shards) {
                if (shard.containsTile(tile)) {
                    return shard.shardId();
                }
            }
            throw new IllegalArgumentException("Tile " + tile + " not owned by any shard");
        }

        /**
         * Non-throwing variant of {@link #findShardForTile}: returns the owning shard id,
         * or {@code -1} if no shard owns the tile (e.g. outside the configured border).
         * Preferred on hot event paths where an unowned tile is normal, not exceptional.
         */
        public long findShardForTileOrUnowned(TilePosition tile) {
            for (var shard : shards) {
                if (shard.containsTile(tile)) {
                    return shard.shardId();
                }
            }
            return -1L;
        }

        public long findShardForChunk(ChunkPosition chunk) {
            TilePosition tile = chunk.toTilePosition();
            return findShardForTile(tile);
        }

        public boolean isInSeam(TilePosition tile) {
            for (var seam : seams) {
                if (seam.overlapsTile(tile)) {
                    return true;
                }
            }
            return false;
        }

        public int getShardCount() {
            return shards.size();
        }
    }

    /**
     * One shard's tile ownership.
     */
    public record ShardPartition(
        long shardId,
        String host,
        String address,
        int minTileX,
        int minTileZ,
        int tileWidth,
        int tileHeight
    ) {
        public boolean containsTile(TilePosition tile) {
            return tile.x() >= minTileX &&
                   tile.x() < minTileX + (tileWidth * TILE_SIZE) &&
                   tile.z() >= minTileZ &&
                   tile.z() < minTileZ + (tileHeight * TILE_SIZE);
        }

        /** Get the tiles this shard is adjacent to (for halo setup) */
        public List<TilePosition> getAdjacentTiles() {
            List<TilePosition> tiles = new ArrayList<>();
            // Top edge
            for (int x = minTileX - 1; x < minTileX + tileWidth; x++) {
                tiles.add(new TilePosition(x * TILE_SIZE, minTileZ - TILE_SIZE));
            }
            // Bottom edge
            for (int x = minTileX - 1; x < minTileX + tileWidth; x++) {
                tiles.add(new TilePosition(x * TILE_SIZE, minTileZ + tileHeight * TILE_SIZE));
            }
            // Left edge
            for (int z = minTileZ - 1; z < minTileZ + tileHeight; z++) {
                tiles.add(new TilePosition(minTileX - TILE_SIZE, z * TILE_SIZE));
            }
            // Right edge
            for (int z = minTileZ - 1; z < minTileZ + tileHeight; z++) {
                tiles.add(new TilePosition(minTileX + tileWidth * TILE_SIZE, z * TILE_SIZE));
            }
            return tiles;
        }

        /** Get the tiles neighboring this shard owns for halo rendering */
        public List<TilePosition> getHaloTiles(PartitionState partition) {
            // Find tiles owned by other shards that are adjacent to this shard
            List<TilePosition> adjacent = getAdjacentTiles();
            return adjacent.stream()
                .filter(t -> !this.containsTile(t))  // not owned by us
                .filter(t -> partition.findShardForTile(t) != shardId)  // owned by another shard
                .collect(Collectors.toList());
        }
    }

    /**
     * A seam boundary between two shards.
     */
    public record SeamBoundary(
        long id,
        String type,  // "horizontal" or "vertical"
        int coordinate,
        int minSecondary,
        int maxSecondary
    ) {
        public boolean overlapsTile(TilePosition tile) {
            return switch (type) {
                case "horizontal" -> coordinate >= tile.z() && coordinate < tile.z() + TILE_SIZE;
                case "vertical" -> coordinate >= tile.x() && coordinate < tile.x() + TILE_SIZE;
                default -> false;
            };
        }
    }

    public PartitionMap() {
        this.state = new PartitionState(
            0,
            Collections.emptyList(),
            Collections.emptyList()
        );
    }

    public PartitionState getState() {
        return state;
    }

    /**
     * Get the current version (for version-gating transfers).
     */
    public long getVersion() {
        return version.get();
    }

    /**
     * Apply a new partition map. Only succeeds if current version matches.
     * This ensures that stale controllers cannot apply conflicting partitions.
     */
    public boolean apply(long expectedVersion, List<ShardPartition> shards, List<SeamBoundary> seams) {
        synchronized (lock) {
            if (version.get() != expectedVersion) {
                return false; // version mismatch — stale controller
            }
            version.incrementAndGet();
            state = new PartitionState(version.get(), shards, seams);
            return true;
        }
    }

    /**
     * Get expected version for apply.
     */
    public long getExpectedVersion() {
        return version.get();
    }

    /**
     * Create a default equal-area partition.
     * Used during initial setup before telemetry-driven repartitioning.
     */
    public static PartitionMap.Builder createDefault(int numShards, String prefix,
                                                     int worldBorderX, int worldBorderZ) {
        return new PartitionMap.Builder(numShards, prefix, worldBorderX, worldBorderZ);
    }

    /**
     * Builder for creating partition maps.
     */
    public static class Builder {
        private final int numShards;
        private final String prefix;
        private List<ShardPartition> shards = new ArrayList<>();
        private List<SeamBoundary> seams = new ArrayList<>();
        private long seamCounter = 0;

        public Builder(int numShards, String prefix, int worldBorderX, int worldBorderZ) {
            this.numShards = numShards;
            this.prefix = prefix;
            // World dimensions (overworld). Derive tile counts from the configured
            // world border; fall back to 128 tiles when the border is unbounded (<= 0).
            int tileStep = 16 * TILE_SIZE; // 4096 blocks per tile row
            int totalTilesX = worldBorderX > 0 ? Math.max(1, (worldBorderX - 1) / tileStep) : 128;
            int totalTilesZ = worldBorderZ > 0 ? Math.max(1, (worldBorderZ - 1) / tileStep) : 128;
            int tileWidth = (int) Math.ceil(Math.sqrt(numShards));
            int tileHeight = (int) Math.ceil((double) totalTilesZ / tileWidth);

            for (int i = 0; i < numShards; i++) {
                int shardX = (i % tileWidth);
                int shardZ = (i / tileWidth);
                int w = (shardX == tileWidth - 1) ?
                    totalTilesX - shardX * 16 :
                    Math.min(16, totalTilesX - shardX * 16);
                int h = (shardZ == tileHeight - 1) ?
                    totalTilesZ - shardZ * 16 :
                    Math.min(16, totalTilesZ - shardZ * 16);

                long shardId = i;
                String addr = prefix + "-" + i + ":25565";
                shards.add(new ShardPartition(
                    shardId,
                    "localhost",
                    addr,
                    shardX * 16 * TILE_SIZE,
                    shardZ * 16 * TILE_SIZE,
                    w,
                    h
                ));

                // Add seam boundary if not last in row/column
                if (shardX < tileWidth - 1) {
                    seams.add(new SeamBoundary(
                        seamCounter++,
                        "vertical",
                        (shardX + 1) * 16 * TILE_SIZE,
                        shardZ * 16 * TILE_SIZE,
                        (shardZ + 1) * 16 * TILE_SIZE
                    ));
                }
                if (shardZ < tileHeight - 1) {
                    seams.add(new SeamBoundary(
                        seamCounter++,
                        "horizontal",
                        (shardZ + 1) * 16 * TILE_SIZE,
                        shardX * 16 * TILE_SIZE,
                        (shardX + 1) * 16 * TILE_SIZE
                    ));
                }
            }
        }

        public PartitionState build() {
            return new PartitionState(0, shards, seams);
        }
    }
}
