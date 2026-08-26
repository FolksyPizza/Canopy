package com.folypizza.canopy.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Per-region metrics collector.
 *
 * Records TPS, MSPT, fluid ticks, entity count, chunk I/O, etc.
 * Per-region — never global. This is the primary telemetry source
 * used by the control plane to decide whether to distribute.
 */
public class MetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    private static final int HISTOGRAM_SIZE = 100;

    private static final ObjectMapper mapper = new ObjectMapper();

    private final long shardId;
    private final ConcurrentHashMap<Long, RegionMetrics> regions = new ConcurrentHashMap<>();
    private final AtomicLong totalRegionsProcessed = new AtomicLong(0);

    public MetricsCollector(long shardId) {
        this.shardId = shardId;
    }

    /**
     * Record a tick completion for a region.
     */
    public void recordTick(long regionKey, double mspt, int fluidTicks, int entityCount, int chunkIOPending) {
        regions.compute(regionKey, (k, existing) -> {
            if (existing == null) {
                existing = new RegionMetrics(k);
            }
            existing.record(mspt, fluidTicks, entityCount, chunkIOPending);
            return existing;
        });

        totalRegionsProcessed.incrementAndGet();
    }

    /**
     * Get a snapshot of all current metrics (for /canopy/metrics.json endpoint).
     */
    public String getMetricsSnapshot() throws JsonProcessingException {
        var regionMetrics = regions.entrySet().stream()
            .map(e -> {
                var m = e.getValue();
                return Map.of(
                    "id", e.getKey(),
                    "mspt_p50", String.format("%.2f", m.msptP50()),
                    "mspt_p99", String.format("%.2f", m.msptP99()),
                    "tps", String.format("%.3f", m.tps()),
                    "fluid_ticks_total", m.fluidTicksTotal(),
                    "fluid_ticks_in_last", m.fluidTicksLastTick(),
                    "entity_count", m.entityCount(),
                    "chunk_io_pending", m.chunkIOPending(),
                    "tile_version", m.tileVersion(),
                    "tick_count", m.tickCount()
                );
            })
            .collect(Collectors.toList());

        double totalCpu = regions.values().stream()
            .mapToDouble(r -> r.msptP50() / 50.0)  // normalize MSPT to ~0-100%
            .sum();

        var result = Map.of(
            "shard_id", shardId,
            "regions", regionMetrics,
            "total_regions", regions.size(),
            "total_ticks_processed", totalRegionsProcessed.get(),
            "region_averages", Map.of(
                "global_tps", String.format("%.3f", calcGlobalTPS()),
                "global_mspt_p50", String.format("%.2f", calcGlobalMsptP50()),
                "total_entity_count", regions.values().stream().mapToInt(RegionMetrics::entityCount).sum(),
                "total_fluid_ticks", regions.values().stream().mapToLong(RegionMetrics::fluidTicksTotal).sum(),
                "pinned_region_threshold_mspt", 50.0
            )
        );

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
    }

    /**
     * Calculate global average TPS (for dashboard only — the gate uses per-region, never global).
     */
    public double calcGlobalTPS() {
        if (regions.isEmpty()) return 20.0;
        return regions.values().stream()
            .mapToDouble(r -> r.tps())
            .average()
            .orElse(20.0);
    }

    public double calcGlobalMsptP50() {
        if (regions.isEmpty()) return 0.0;
        return regions.values().stream()
            .mapToDouble(r -> r.msptP50())
            .average()
            .orElse(0.0);
    }

    /**
     * Check if global CPU is saturated.
     */
    public boolean isGlobalCpuSaturated(double totalCores) {
        double avgMspt = calcGlobalMsptP50();
        return avgMspt > 10.0 / totalCores; // average MSPT > 10ms per core
    }

    /**
     * Get max per-region MSPT (used for gating check).
     */
    public double getMaxPerRegionMspt() {
        return regions.values().stream()
            .mapToDouble(RegionMetrics::msptP50)
            .max()
            .orElse(0.0);
    }

    /**
     * Get the number of loaded regions (for sizing calculation).
     */
    public int getLoadedRegionCount() {
        return regions.size();
    }

    /** Count of regions whose p50 MSPT exceeds the given threshold. */
    public int countRegionsAbove(double msptThreshold) {
        return (int) regions.values().stream()
            .filter(r -> r.msptP50() > msptThreshold)
            .count();
    }

    /** Percentile (0-100) over the per-region p50 MSPT values. */
    public double regionMsptPercentile(double percentile) {
        double[] vals = regions.values().stream()
            .mapToDouble(RegionMetrics::msptP50)
            .sorted()
            .toArray();
        if (vals.length == 0) return 0.0;
        int idx = (int) Math.ceil(percentile / 100.0 * vals.length) - 1;
        idx = Math.max(0, Math.min(vals.length - 1, idx));
        return vals[idx];
    }

    /** Per-region MSPT rows (regionKey -> p50), sorted worst-first. */
    public List<Map<String, Object>> regionMsptRows() {
        return regions.entrySet().stream()
            .map(e -> {
                Map<String, Object> m = new java.util.LinkedHashMap<String, Object>();
                m.put("region", e.getKey());
                m.put("mspt", Math.round(e.getValue().msptP50() * 100.0) / 100.0);
                m.put("entities", e.getValue().entityCount());
                return m;
            })
            .sorted((a, b) -> Double.compare((double) b.get("mspt"), (double) a.get("mspt")))
            .collect(Collectors.toList());
    }

    /**
     * Per-region metrics.
     */
    public static class RegionMetrics {
        private final long regionKey;
        private final long[] msptHistory = new long[HISTOGRAM_SIZE];
        private final long[] fluidTicksHistory = new long[HISTOGRAM_SIZE];
        private int historyIdx = 0;
        private int writeCount = 0;
        private long fluidTicksTotal = 0;
        private long tileVersion = 1;
        private int tickCount = 0;
        private int entityCount = 0;
        private int chunkIOPending = 0;

        public RegionMetrics(long regionKey) {
            this.regionKey = regionKey;
            // Fill with default 50ms (off) value
            for (int i = 0; i < HISTOGRAM_SIZE; i++) {
                msptHistory[i] = 50_000_000; // 50ms in nanoseconds
            }
        }

        public synchronized void record(double mspt, int fluidTicks, int entityCount, int chunkIOPending) {
            int msptNs = (int) (mspt * 1_000_000); // convert to nanoseconds
            msptHistory[historyIdx] = msptNs;
            fluidTicksHistory[historyIdx] = fluidTicks;
            historyIdx = (historyIdx + 1) % HISTOGRAM_SIZE;
            if (writeCount < HISTOGRAM_SIZE) writeCount++;
            fluidTicksTotal += fluidTicks;
            this.entityCount = entityCount;
            this.chunkIOPending = chunkIOPending;
            this.tickCount++;
        }

        public double tps() {
            // TPS = ticks processed per second
            // In Folia, we calculate from the average MSPT
            double avgMspt = ArrayStats.p50(msptHistory, writeCount) / 1_000_000.0; // back to ms
            if (avgMspt <= 0) return 20.0;
            return Math.min(20.0, 1000.0 / avgMspt);
        }

        public double msptP50() {
            return ArrayStats.p50(msptHistory, writeCount) / 1_000_000.0;
        }

        public double msptP99() {
            return ArrayStats.p99(msptHistory, writeCount) / 1_000_000.0;
        }

        public int fluidTicksTotal() { return (int) fluidTicksTotal; }
        public int fluidTicksLastTick() { return writeCount > 0 ? (int) fluidTicksHistory[historyIdx] : 0; }
        public int entityCount() { return entityCount; }
        public int chunkIOPending() { return chunkIOPending; }
        public long tileVersion() { return tileVersion; }
        public int tickCount() { return tickCount; }
        public void increaseTileVersion() { tileVersion++; }
    }

    /**
     * Simple array statistics helper.
     */
    public static class ArrayStats {
        private static final long[] TEMP = new long[HISTOGRAM_SIZE];

        public static double p50(long[] arr, int count) {
            if (count == 0) return 0;
            System.arraycopy(arr, 0, TEMP, 0, Math.min(count, HISTOGRAM_SIZE));
            java.util.Arrays.sort(TEMP, 0, count);
            return TEMP[count / 2];
        }

        public static double p99(long[] arr, int count) {
            if (count == 0) return 0;
            System.arraycopy(arr, 0, TEMP, 0, Math.min(count, HISTOGRAM_SIZE));
            java.util.Arrays.sort(TEMP, 0, count);
            return TEMP[(int) (count * 0.99)];
        }
    }
}
