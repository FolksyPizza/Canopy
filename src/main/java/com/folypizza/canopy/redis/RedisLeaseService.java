package com.folypizza.canopy.redis;

import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisLeaseService {
    private static final Logger log = LoggerFactory.getLogger(RedisLeaseService.class);
    private static final long DEFAULT_LEASE_TTL_MS = 30_000;
    private static final String LEASE_PREFIX = "canopy:lease:";

    private final RedisClient redisClient;

    public RedisLeaseService(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public boolean acquire(String leaseKey, long shardId, long timeoutMs) {
        String key = LEASE_PREFIX + leaseKey;
        long timeoutSec = Math.max(1, timeoutMs / 1000);
        String value = shardId + ":" + System.currentTimeMillis();
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            String setResult = commands.set(key, value,
                io.lettuce.core.SetArgs.Builder.nx().ex((int) timeoutSec));
            return "OK".equals(setResult);
        } catch (Exception e) {
            log.warn("Lease acquire failed for {}: {}", leaseKey, e.getMessage());
            return false;
        }
    }

    public boolean renew(String leaseKey, long shardId) {
        String key = LEASE_PREFIX + leaseKey;
        String value = shardId + ":" + System.currentTimeMillis();
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            String setResult = commands.set(key, value,
                io.lettuce.core.SetArgs.Builder.nx().ex((int) (DEFAULT_LEASE_TTL_MS / 1000)));
            return "OK".equals(setResult);
        } catch (Exception e) {
            log.warn("Lease renew failed for {}: {}", leaseKey, e.getMessage());
            return false;
        }
    }

    public Boolean release(String leaseKey, long shardId) {
        String key = LEASE_PREFIX + leaseKey;
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            long deleted = commands.del(key);
            return deleted > 0;
        } catch (Exception e) {
            log.warn("Lease release failed for {}: {}", leaseKey, e.getMessage());
            return false;
        }
    }

    public long whoOwns(String leaseKey) {
        String key = LEASE_PREFIX + leaseKey;
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            String value = commands.get(key);
            if (value != null && value.contains(":")) {
                return Long.parseLong(value.split(":")[0]);
            }
        } catch (Exception e) {
            log.warn("Lease query failed for {}: {}", leaseKey, e.getMessage());
        }
        return -1;
    }

    public boolean twoPhaseHandoff(String leaseKey, long sourceShard, long destShard) {
        String key = LEASE_PREFIX + leaseKey;
        String expected = sourceShard + ":";
        String newValue = destShard + ":" + System.currentTimeMillis();
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            String existing = commands.get(key);
            if (existing != null && existing.startsWith(expected)) {
                String setResult = commands.set(key, newValue,
                    io.lettuce.core.SetArgs.Builder.nx().ex(30));
                return "OK".equals(setResult);
            }
            return false;
        } catch (Exception e) {
            log.warn("Two-phase handoff failed for {}: {}", leaseKey, e.getMessage());
            return false;
        }
    }

    public boolean rollback(String leaseKey, long targetShard) {
        String key = LEASE_PREFIX + leaseKey;
        String value = targetShard + ":" + System.currentTimeMillis();
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            String setResult = commands.set(key, value,
                io.lettuce.core.SetArgs.Builder.nx().ex(30));
            return "OK".equals(setResult);
        } catch (Exception e) {
            log.warn("Lease rollback failed for {}: {}", leaseKey, e.getMessage());
            return false;
        }
    }

    public boolean forceExpire(String leaseKey) {
        String key = LEASE_PREFIX + leaseKey;
        try (var connection = redisClient.connect()) {
            var commands = connection.sync();
            long deleted = commands.del(key);
            return deleted > 0;
        } catch (Exception e) {
            log.warn("Lease force expire failed for {}: {}", leaseKey, e.getMessage());
            return false;
        }
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }
}
