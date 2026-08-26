package com.folypizza.canopy.repartition;

import com.folypizza.canopy.graph.PartitionAdvisor;
import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.metrics.MetricsCollector;
import com.folypizza.canopy.model.ChunkPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AutoRepartitionEngine {
    private static final Logger log = LoggerFactory.getLogger(AutoRepartitionEngine.class);
    private static final long COOLDOWN_MS = 300_000;
    private static final double IMBALANCE_THRESHOLD = 0.2;
    private static final int MONITOR_INTERVAL_MS = 15_000;

    public record PartitionPlan(
        Map<Long, ShardAssignment> assignments,
        double improvementPercent,
        long plannedAt
    ) {}

    public record ShardAssignment(
        long shardId,
        Set<ChunkPosition> ownedChunks,
        String host,
        String address
    ) {}

    private final MetricsCollector metricsCollector;
    private final ConcurrentHashMap<Long, PartitionPlan> partitionMap;
    private final PartitionAdvisor partitionAdvisor;
    private final AtomicLong lastRebalanceMs = new AtomicLong(0);
    private volatile boolean running = false;
    private volatile boolean killSwitch = false;
    private final java.util.Deque<Map<String, Object>> evalHistory = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private final ScheduledExecutorService executor;

    public static final double IMBALANCE_THRESHOLD_PUBLIC = IMBALANCE_THRESHOLD;

    public AutoRepartitionEngine(
        MetricsCollector metricsCollector,
        PartitionMap partitionMap,
        PartitionAdvisor partitionAdvisor
    ) {
        this.metricsCollector = metricsCollector;
        this.partitionMap = new ConcurrentHashMap<>();
        this.partitionAdvisor = partitionAdvisor;
        this.executor = Executors.newScheduledThreadPool(1, r -> {
            var t = new Thread(r, "canopy-repartition");
            t.setDaemon(true);
            return t;
        });
    }

    public void startMonitoring() {
        running = true;
        executor.scheduleAtFixedRate(this::tick, MONITOR_INTERVAL_MS, MONITOR_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("AutoRepartitionEngine started (interval={}ms)", MONITOR_INTERVAL_MS);
    }

    public void stop() {
        running = false;
        executor.shutdown();
    }

    private void tick() {
        if (!running) return;
        double imbalance = computeImbalance();
        boolean triggered = imbalance >= IMBALANCE_THRESHOLD;
        boolean cooling = System.currentTimeMillis() - lastRebalanceMs.get() < COOLDOWN_MS;
        String decision;
        if (killSwitch) {
            decision = "kill-switch: auto-rebalance disabled";
        } else if (!triggered) {
            decision = "below threshold";
        } else if (cooling) {
            decision = "in cooldown";
        } else {
            decision = "rebalance triggered";
        }
        recordEval(imbalance, triggered, decision);

        if (killSwitch || !triggered || cooling) return;
        log.info("Rebalance imbalance detected ({}), computing new partition...", imbalance);
        var newPlan = computeNewPartition();
        if (newPlan != null) {
            applyPartition(newPlan);
        }
    }

    private void recordEval(double imbalance, boolean triggered, String decision) {
        Map<String, Object> e = new java.util.LinkedHashMap<>();
        e.put("imbalance", Math.round(imbalance * 1000.0) / 1000.0);
        e.put("threshold", IMBALANCE_THRESHOLD);
        e.put("triggered", triggered);
        e.put("decision", decision);
        e.put("uptimeMs", System.currentTimeMillis() - lastRebalanceMs.get());
        evalHistory.addFirst(e);
        while (evalHistory.size() > 50) evalHistory.removeLast();
    }

    // --- Governance surface for the control plane ---

    public boolean isKillSwitch() { return killSwitch; }

    public void setKillSwitch(boolean value) {
        this.killSwitch = value;
        log.warn("Auto-rebalance kill switch set to {}", value);
    }

    public double getThreshold() { return IMBALANCE_THRESHOLD; }

    public long getCooldownRemainingMs() {
        long elapsed = System.currentTimeMillis() - lastRebalanceMs.get();
        return Math.max(0, COOLDOWN_MS - elapsed);
    }

    public java.util.List<Map<String, Object>> getEvalHistory() {
        return new java.util.ArrayList<>(evalHistory);
    }

    public boolean checkImbalance() {
        double imbalance = computeImbalance();
        return imbalance >= IMBALANCE_THRESHOLD;
    }

    public boolean shouldTriggerRebalance() {
        if (System.currentTimeMillis() - lastRebalanceMs.get() < COOLDOWN_MS) {
            return false;
        }
        return checkImbalance();
    }

    public PartitionPlan computeNewPartition() {
        var currentState = partitionMap.values().stream()
            .map(p -> p)
            .findFirst()
            .orElse(null);
        if (currentState == null) {
            log.warn("No partition state available for rebalancing");
            return null;
        }
        var newState = partitionAdvisor.computePartition();
        Map<Long, ShardAssignment> assignments = new HashMap<>();
        for (var shard : newState.shards()) {
            var assignment = new ShardAssignment(
                shard.shardId(),
                Collections.emptySet(),
                shard.host(),
                shard.address()
            );
            assignments.put(shard.shardId(), assignment);
        }
        double improvement = computeImprovePercent(currentState, newState);
        var plan = new PartitionPlan(assignments, improvement, System.currentTimeMillis());
        partitionMap.put(System.currentTimeMillis(), plan);
        lastRebalanceMs.set(System.currentTimeMillis());
        log.info("Computed new partition with {}% improvement", String.format("%.2f", improvement));
        return plan;
    }

    public double computeImbalance() {
        var stats = metricsCollector;
        double maxMspt = metricsCollector.getMaxPerRegionMspt();
        double globalAvgMspt = metricsCollector.calcGlobalMsptP50();
        if (globalAvgMspt <= 0) return 0.0;
        return (maxMspt - globalAvgMspt) / globalAvgMspt;
    }

    public void applyPartition(PartitionPlan plan) {
        log.info("Applying partition plan with {} assignments", plan.assignments().size());
        lastRebalanceMs.set(System.currentTimeMillis());
    }

    private double computeImprovePercent(PartitionPlan oldPlan, PartitionMap.PartitionState newState) {
        if (oldPlan == null) return 0.0;
        return Math.max(0.0, 1.0 - (double) newState.shards().size() / oldPlan.assignments().size());
    }

    private void runRebalance(PartitionPlan plan) {
        log.info("Rebalance completed with {}% improvement", plan.improvementPercent());
    }
}
