package com.folypizza.canopy.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Tracks registered shard processes and their health.
 * Acts as the service discovery and health monitoring layer.
 */
public class ShardRegistry {
    private static final Logger log = LoggerFactory.getLogger(ShardRegistry.class);
    private static final long HEARTBEAT_INTERVAL_MS = 5000;
    private static final long HEARTBEAT_TIMEOUT_MS = 15000;

    private final ConcurrentHashMap<Long, ShardEntry> shards = new ConcurrentHashMap<>();
    private final AtomicLong tickCounter = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public ShardRegistry() {
        startHeartbeat();
    }

    /**
     * Register or re-register a shard.
     */
    public boolean register(ShardEntry shard) {
        shard.lastHeartbeat(System.currentTimeMillis());
        shards.put(shard.shardId(), shard);
        log.info("Registered shard {} at {} ({} regions)", 
            shard.shardId(), shard.address(), shard.regionCount());
        return true;
    }

    /**
     * Unregister a shard (on shutdown).
     */
    public boolean unregister(long shardId) {
        ShardEntry removed = shards.remove(shardId);
        if (removed != null) {
            log.info("Unregistered shard {}", shardId);
            return true;
        }
        return false;
    }

    /**
     * Update an existing shard's heartbeat.
     */
    public boolean heartbeat(long shardId, double tps, double mspt, int playerCount) {
        ShardEntry entry = shards.get(shardId);
        if (entry == null) {
            return false; // doesn't exist
        }
        entry.lastHeartbeat(System.currentTimeMillis());
        entry.tps(tps);
        entry.mspt(mspt);
        entry.playerCount(playerCount);
        return true;
    }

    /**
     * Get all registered shards.
     */
    public Collection<ShardEntry> listShards() {
        return Collections.unmodifiableCollection(shards.values());
    }

    /**
     * Get all healthy shards.
     */
    public Collection<ShardEntry> getHealthyShards() {
        long now = System.currentTimeMillis();
        return shards.values().stream()
            .filter(s -> now - s.lastHeartbeat() < HEARTBEAT_TIMEOUT_MS)
            .collect(Collectors.toList());
    }

    /**
     * Get a specific shard by ID (may be null).
     */
    public ShardEntry getShard(long shardId) {
        return shards.get(shardId);
    }

    /**
     * Find a healthy shard that owns a given tile.
     */
    public Optional<ShardEntry> findShardForTile(long tileX, long tileZ) {
        // This is a placeholder — the actual implementation would check
        // the partition map to find which shard owns this tile
        // Then verify it's healthy
        return shards.values().stream()
            .filter(s -> s.address() != null)
            .findFirst();
    }

    /**
     * Count of registered shards.
     */
    public int getShardCount() {
        return shards.size();
    }

    /**
     * Count of healthy shards.
     */
    public int getHealthyShardCount() {
        return getHealthyShards().size();
    }

    public boolean isHealthy() {
        return getHealthyShardCount() > 0;
    }

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long before = shards.size();
            long healthy = getHealthyShards().size();
            tickCounter.incrementAndGet();
            if (before > healthy) {
                log.debug("Shard heartbeat check: {} total, {} healthy", before, healthy);
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    /**
     * A registered shard's state.
     */
    public static class ShardEntry {
        private final long shardId;
        private final String host;
        private String address;
        private double tps;
        private double mspt;
        private int regionCount;
        private int playerCount;
        private int entityCount;
        private long lastHeartbeat;
        private volatile boolean healthy;

        public ShardEntry(long shardId, String host, String address, int regionCount) {
            this.shardId = shardId;
            this.host = host;
            this.address = address;
            this.regionCount = regionCount;
            this.lastHeartbeat = System.currentTimeMillis();
            this.healthy = true;
            this.tps = -1;
            this.mspt = -1;
            this.playerCount = 0;
            this.entityCount = 0;
        }

        public long shardId() { return shardId; }
        public String host() { return host; }
        public String address() { return address; }
        public void address(String address) { this.address = address; }
        public double tps() { return tps; }
        public void tps(double tps) { this.tps = tps; }
        public double mspt() { return mspt; }
        public void mspt(double mspt) { this.mspt = mspt; }
        public int regionCount() { return regionCount; }
        public void regionCount(int regionCount) { this.regionCount = regionCount; }
        public int playerCount() { return playerCount; }
        public void playerCount(int playerCount) { this.playerCount = playerCount; }
        public int entityCount() { return entityCount; }
        public void entityCount(int entityCount) { this.entityCount = entityCount; }
        public long lastHeartbeat() { return lastHeartbeat; }
        public void lastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
        public boolean isHealthy() {
            return System.currentTimeMillis() - lastHeartbeat < HEARTBEAT_TIMEOUT_MS;
        }
    }

    // Java 21 record would be cleaner but this is a placeholder to keep things simple
    // The ShardEntry class could be rewritten as a record once the core is stable
}
