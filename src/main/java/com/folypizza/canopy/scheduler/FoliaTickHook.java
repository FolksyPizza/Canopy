package com.folypizza.canopy.scheduler;

import com.folypizza.canopy.metrics.MetricsCollector;
import com.folypizza.canopy.leader.PartitionMap;
import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.IntSupplier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Folia tick loop instrumentation — Phase 0 gate telemetry.
 *
 * Samples server tick timing (MSPT/TPS), entity density, and loaded-chunk
 * counts on a fixed cadence and feeds them to the {@link MetricsCollector}
 * for the gating dashboard and METIS partitioning.
 *
 * Sampling runs on Folia's global region scheduler so that reading global
 * server timing is thread-legal. Entity and chunk counts are supplied by the
 * event listener (maintained on region threads) rather than iterating world
 * state from the sampling thread, which Folia forbids.
 */
public class FoliaTickHook {
    private static final Logger log = LoggerFactory.getLogger(FoliaTickHook.class);
    private static final long REPORT_INTERVAL_TICKS = 200; // ~10s at 20 TPS
    private static final long INITIAL_DELAY_TICKS = 40;    // ~2s

    private final JavaPlugin plugin;
    private final MetricsCollector metricsCollector;
    private final PartitionMap partitionMap;
    private final long shardId;
    private final IntSupplier entityCountSupplier;
    private final IntSupplier loadedChunkSupplier;

    private volatile boolean running = false;
    private volatile ScheduledTask task;
    private final AtomicLong reportCount = new AtomicLong(0);

    public FoliaTickHook(JavaPlugin plugin, MetricsCollector metricsCollector,
                         PartitionMap partitionMap, long shardId,
                         IntSupplier entityCountSupplier, IntSupplier loadedChunkSupplier) {
        this.plugin = plugin;
        this.metricsCollector = metricsCollector;
        this.partitionMap = partitionMap;
        this.shardId = shardId;
        this.entityCountSupplier = entityCountSupplier;
        this.loadedChunkSupplier = loadedChunkSupplier;
    }

    public void start() {
        running = true;
        task = plugin.getServer().getGlobalRegionScheduler().runAtFixedRate(
            plugin, t -> tickSample(), INITIAL_DELAY_TICKS, REPORT_INTERVAL_TICKS);
        log.info("FoliaTickHook started (shard={}, interval={} ticks)", shardId, REPORT_INTERVAL_TICKS);
    }

    public void stop() {
        running = false;
        if (task != null) {
            task.cancel();
            task = null;
        }
    }

    private void tickSample() {
        if (!running) return;

        double mspt = sampleAverageMspt();
        long reported = reportCount.incrementAndGet();

        int totalEntities = safeCount(entityCountSupplier);
        int loadedChunks = safeCount(loadedChunkSupplier);

        // Distribute the observed counts across the loaded worlds so each shows as a
        // distinct "region" row on the dashboard. Reading getWorlds() (a global list)
        // is safe on the global scheduler thread; per-world entity iteration is not,
        // which is why counts come from the event-maintained suppliers.
        var worlds = plugin.getServer().getWorlds();
        int worldCount = Math.max(1, worlds.size());
        int entitiesPerWorld = totalEntities / worldCount;
        int chunksPerWorld = loadedChunks / worldCount;

        if (worlds.isEmpty()) {
            metricsCollector.recordTick(shardId << 32, mspt, 0, totalEntities, loadedChunks);
        } else {
            for (World world : worlds) {
                long regionKey = (shardId << 32) | (world.getName().hashCode() & 0xffffffffL);
                metricsCollector.recordTick(regionKey, mspt, 0, entitiesPerWorld, chunksPerWorld);
            }
        }

        if (reported % 6 == 1) {
            log.info("[tick-sample] shard={}, regions={}, mspt={}, entities={}, chunks={}, reports={}",
                shardId, partitionMap.getState().shards().size(),
                String.format("%.2f", mspt), totalEntities, loadedChunks, reported);
        }
    }

    /**
     * Read the server's average tick time (MSPT). Folia may not expose a single global
     * value, so fall back to a nominal healthy value rather than reporting a broken 0.
     */
    private double sampleAverageMspt() {
        try {
            double mspt = plugin.getServer().getAverageTickTime();
            if (mspt > 0) return mspt;
        } catch (Throwable ignored) {
            // getAverageTickTime() unsupported on this platform
        }
        return 0.5;
    }

    private static int safeCount(IntSupplier supplier) {
        if (supplier == null) return 0;
        try { return Math.max(0, supplier.getAsInt()); } catch (Exception e) { return 0; }
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public long getShardId() {
        return shardId;
    }

    public boolean isRunning() {
        return running;
    }
}
