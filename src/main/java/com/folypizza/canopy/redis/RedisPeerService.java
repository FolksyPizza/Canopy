package com.folypizza.canopy.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisPeerService {
    private static final Logger log = LoggerFactory.getLogger(RedisPeerService.class);
    private static final String HEARTBEAT_CHANNEL = "canopy:heartbeat";
    private static final String SHARD_LIST_CHANNEL = "canopy:shard-list";
    private static final long HEARTBEAT_INTERVAL_MS = 5_000;
    private static final long HEARTBEAT_TTL_MS = 15_000;

    private final RedisClient redisClient;
    private final long localShardId;
    private final StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private final StatefulRedisConnection<String, String> commandConnection;
    private final ConcurrentHashMap<Long, Long> activeShards = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService executor;

    public RedisPeerService(RedisClient redisClient, long localShardId) {
        this.redisClient = redisClient;
        this.localShardId = localShardId;
        this.pubSubConnection = redisClient.connectPubSub();
        this.commandConnection = redisClient.connect();
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            var t = new Thread(r, "canopy-redis-peer");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        pubSubConnection.addListener(new RedisPubSubAdapter<String, String>() {
            @Override
            public void message(String channel, String message) {
                if (channel.equals(HEARTBEAT_CHANNEL)) {
                    handleHeartbeat(message);
                }
                if (channel.equals(SHARD_LIST_CHANNEL)) {
                    log.debug("Shard list update: {}", message);
                }
            }
        });
        pubSubConnection.sync().subscribe(HEARTBEAT_CHANNEL, SHARD_LIST_CHANNEL);
        executor.scheduleAtFixedRate(this::broadcastHeartbeat,
            HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(this::staleShardCleanup,
            HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("RedisPeerService started (shard={}, channel={})", localShardId, HEARTBEAT_CHANNEL);
    }

    public void stop() {
        running.set(false);
        pubSubConnection.close();
        commandConnection.close();
        executor.shutdown();
    }

    public void broadcastHeartbeat() {
        if (!running.get()) return;
        String payload = localShardId + ":" + System.currentTimeMillis();
        commandConnection.sync().publish(HEARTBEAT_CHANNEL, payload);
        commandConnection.sync().publish(SHARD_LIST_CHANNEL, payload);
    }

    public List<Long> discoverActiveShards() {
        var snapshot = new HashMap<>(activeShards);
        long now = System.currentTimeMillis();
        return snapshot.entrySet().stream()
            .filter(e -> now - e.getValue() < HEARTBEAT_TTL_MS)
            .map(Map.Entry::getKey)
            .toList();
    }

    private void handleHeartbeat(String message) {
        var parts = message.split(":");
        if (parts.length < 2) return;
        try {
            long shardId = Long.parseLong(parts[0]);
            if (shardId == localShardId) return;
            long timestamp = Long.parseLong(parts[1]);
            activeShards.put(shardId, timestamp);
        } catch (NumberFormatException e) {
            log.warn("Invalid heartbeat message: {}", message);
        }
    }

    private void staleShardCleanup() {
        long now = System.currentTimeMillis();
        activeShards.entrySet().removeIf(e -> now - e.getValue() > HEARTBEAT_TTL_MS);
    }

    public int getActiveShardCount() {
        return activeShards.size();
    }

    public boolean isRunning() {
        return running.get();
    }
}
