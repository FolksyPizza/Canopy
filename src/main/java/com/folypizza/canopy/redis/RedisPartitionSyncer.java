package com.folypizza.canopy.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RedisPartitionSyncer {
    private static final Logger log = LoggerFactory.getLogger(RedisPartitionSyncer.class);
    private static final String PARTITION_VERSION_KEY = "canopy:partition:version";
    private static final String PARTITION_DATA_KEY = "canopy:partition:data";
    private static final int VERSION_WAIT_MAX_MS = 30_000;

    private final RedisStringCommands<String, String> redis;
    private final AtomicLong localVersion = new AtomicLong(0);
    private final AtomicReference<String> localData = new AtomicReference<>(null);

    public RedisPartitionSyncer(RedisClient redisClient) {
        this.redis = redisClient.connect(StringCodec.UTF8).sync();
    }

    public long getPartitionVersion() {
        String result = redis.get(PARTITION_VERSION_KEY);
        if (result != null) {
            try { return Long.parseLong(result); } catch (NumberFormatException e) { /* fall through */ }
        }
        return localVersion.get();
    }

    public long bumpPartitionVersion(String data) {
        long newVersion = localVersion.incrementAndGet();
        localData.set(data);
        redis.set(PARTITION_VERSION_KEY, String.valueOf(newVersion));
        redis.set(PARTITION_DATA_KEY, data);
        log.info("Bumped partition version to {}", newVersion);
        return newVersion;
    }

    public boolean syncPartition(String data) {
        long currentVersion = getPartitionVersion();
        bumpPartitionVersion(data);
        log.info("Partition sync complete (old={} -> {})", currentVersion, localVersion.get());
        return true;
    }

    public String getRemotePartition() {
        return redis.get(PARTITION_DATA_KEY);
    }

    public boolean waitVersion(long targetVersion) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < VERSION_WAIT_MAX_MS) {
            long current = getPartitionVersion();
            if (current >= targetVersion) return true;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        log.warn("Timeout waiting for partition version {} (current={})", targetVersion, getPartitionVersion());
        return false;
    }

    public long getLocalVersion() {
        return localVersion.get();
    }
}
