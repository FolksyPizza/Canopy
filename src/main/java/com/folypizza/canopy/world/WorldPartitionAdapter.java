package com.folypizza.canopy.world;

import com.folypizza.canopy.metrics.MetricsCollector;
import com.folypizza.canopy.halo.TileUpdateTracker;
import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.model.TilePosition;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorldPartitionAdapter {
    private static final Logger log = LoggerFactory.getLogger(WorldPartitionAdapter.class);
    private static final int TILE_SIZE = 256;

    private final JavaPlugin plugin;
    private final MetricsCollector metricsCollector;
    private final PartitionMap partitionMap;
    private final TileUpdateTracker tileTracker;
    private final ConcurrentHashMap<String, WorldMetrics> worldMetrics = new ConcurrentHashMap<>();

    public WorldPartitionAdapter(JavaPlugin plugin, MetricsCollector metricsCollector,
                                  PartitionMap partitionMap, TileUpdateTracker tileTracker) {
        this.plugin = plugin;
        this.metricsCollector = metricsCollector;
        this.partitionMap = partitionMap;
        this.tileTracker = tileTracker;
    }

    /**
     * Called by the tick loop to record per-region metrics.
     */
    public void onRegionTick(long regionKey, double mspt, int fluidTicks, int entityCount) {
        metricsCollector.recordTick(regionKey, mspt, fluidTicks, entityCount, 0);
        worldMetrics.compute(String.valueOf(regionKey), (k, existing) -> {
            if (existing == null) {
                return WorldMetrics.of(mspt, fluidTicks, entityCount);
            }
            return existing.withAddition(mspt, fluidTicks, entityCount);
        });
    }

    /**
     * Called when a block changes. Updates tile version in the tracker.
     * Returns shard ID that owns this tile, or -1 if unowned.
     */
    public long onBlockChange(int blockX, int blockZ, Block block) {
        TilePosition tilePos = TilePosition.fromBlockCoords(blockX, blockZ);
        long shardId = partitionMap.getState().findShardForTileOrUnowned(tilePos);
        tileTracker.updateTileVersion(tilePos, shardId);
        return shardId;
    }

    /**
     * Called when a chunk is loaded or unloaded.
     */
    public void onChunkLoaded(int chunkX, int chunkZ, String worldName) {
        TilePosition tilePos = TilePosition.fromBlockCoords(chunkX * 16, chunkZ * 16);
        long shardId = partitionMap.getState().findShardForTileOrUnowned(tilePos);
        tileTracker.onChunkEvent(chunkX, chunkZ, true);
    }

    public void onChunkUnloaded(int chunkX, int chunkZ, String worldName) {
        tileTracker.onChunkEvent(chunkX, chunkZ, false);
    }

    /**
     * Determine which shard should handle a given chunk.
     */
    public long findShardForChunk(int chunkX, int chunkZ) {
        TilePosition tilePos = TilePosition.fromBlockCoords(chunkX * 16, chunkZ * 16);
        return partitionMap.getState().findShardForTileOrUnowned(tilePos);
    }

    /**
     * Determine which shard should handle a given block position.
     */
    public long findShardForBlock(int blockX, int blockZ) {
        TilePosition tilePos = TilePosition.fromBlockCoords(blockX, blockZ);
        return partitionMap.getState().findShardForTileOrUnowned(tilePos);
    }

    /**
     * Convert block coordinates to tile position.
     */
    public TilePosition toTile(int blockX, int blockZ) {
        return TilePosition.fromBlockCoords(blockX, blockZ);
    }

    /**
     * Estimate the load of a world by summing loaded chunk count.
     */
    public int estimateRegionLoad(World world) {
        if (world == null) return 0;
        return world.getLoadedChunks().length;
    }

    /**
     * Get metrics snapshot for all worlds.
     */
    @SuppressWarnings("unchecked")
    public Map<String, WorldMetrics> getWorldMetrics() {
        return Map.copyOf(worldMetrics);
    }

    /**
     * Get the partition map this adapter is using.
     */
    public PartitionMap getPartitionMap() {
        return partitionMap;
    }

    public TileUpdateTracker getTileTracker() {
        return tileTracker;
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public JavaPlugin getPlugin() {
        return plugin;
    }

    public record WorldMetrics(double totalMspt, int totalFluidTicks, int totalEntityCount,
                               int samples, double lastMspt, int lastFluidTicks, int lastEntityCount) {
        public static WorldMetrics of(double mspt, int fluidTicks, int entityCount) {
            return new WorldMetrics(mspt, fluidTicks, entityCount, 1, mspt, fluidTicks, entityCount);
        }
        public WorldMetrics withAddition(double mspt, int fluidTicks, int entityCount) {
            double newTotalMspt = totalMspt * samples + mspt;
            int newTotalFluid = totalFluidTicks + fluidTicks;
            int newTotalEntity = totalEntityCount + entityCount;
            int newSamples = samples + 1;
            return new WorldMetrics(
                newTotalMspt, newTotalFluid, newTotalEntity, newSamples,
                mspt, fluidTicks, entityCount
            );
        }
    }
}
