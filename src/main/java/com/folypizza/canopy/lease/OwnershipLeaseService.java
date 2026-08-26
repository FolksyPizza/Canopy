package com.folypizza.canopy.lease;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis-backed (simulation: in-memory) ownership lease system for entity and tile transfers.
 *
 * Provides:
 * 1. Atomic lease acquisition with CAS (compare-and-swap)
 * 2. Lease renewal with TTL
 * 3. Two-phase handoff for entity migration
 * 4. "Rollback over duplication" invariant
 */
public class OwnershipLeaseService {
    private static final Logger log = LoggerFactory.getLogger(OwnershipLeaseService.class);
    private static final long DEFAULT_LEASE_TTL_MS = 30_000;
    private static final long RENEWAL_WINDOW_MS = 15_000;
    private static final String LEASE_PREFIX = "canopy:lease:";
    private static final String SEAT_PREFIX = "canopy:seat:";

    private final ConcurrentHashMap<String, LeaseEntry> leases = new ConcurrentHashMap<>();
    private final AtomicLong generation = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public OwnershipLeaseService() {
        startLeaseExpirationCheck();
    }

    /**
     * Acquire a lease with CAS.
     * Returns true if we got the lease, false if someone else already held it.
     */
    public boolean acquire(String leaseKey, long newShardId, long expectedNonce) {
        String key = LEASE_PREFIX + leaseKey;
        AtomicBoolean acquired = new AtomicBoolean(false);
        leases.compute(key, (k, existing) -> {
            if (existing == null || isExpired(existing)) {
                long newGen = generation.incrementAndGet();
                acquired.set(true);
                log.debug("Lease acquired for {} by shard {}", leaseKey, newShardId);
                return new LeaseEntry(newShardId, newGen);
            }
            if (existing.generation() == expectedNonce || expectedNonce == -1) {
                long newGen = generation.incrementAndGet();
                acquired.set(true);
                log.debug("Lease acquired for {} by shard {} (CAS success)", leaseKey, newShardId);
                return new LeaseEntry(newShardId, newGen);
            }
            log.debug("Lease denied for {} (shard {} already holds it)", leaseKey, existing.shardId());
            return existing;
        });
        return acquired.get();
    }

    public boolean renew(String leaseKey, long shardId) {
        String key = LEASE_PREFIX + leaseKey;
        return leases.compute(key, (k, entry) -> {
            if (entry == null || entry.shardId() != shardId) {
                return entry;
            }
            log.debug("Lease renewed for {} by shard {}", key, shardId);
            return new LeaseEntry(entry.shardId(), entry.generation());
        }) != null;
    }

    public boolean release(String leaseKey, long shardId) {
        String key = LEASE_PREFIX + leaseKey;
        LeaseEntry entry = leases.get(key);
        if (entry == null || entry.shardId() != shardId) {
            return false;
        }
        leases.remove(key, entry);
        return true;
    }

    public long whoOwns(String leaseKey) {
        String key = LEASE_PREFIX + leaseKey;
        LeaseEntry entry = leases.get(key);
        if (entry == null || isExpired(entry)) {
            return -1;
        }
        return entry.shardId();
    }

    public boolean twoPhaseHandoff(String leaseKey, long sourceShard, long destShard) {
        String key = LEASE_PREFIX + leaseKey;
        AtomicBoolean transferred = new AtomicBoolean(false);
        leases.compute(key, (k, existing) -> {
            if (existing != null && existing.shardId() == sourceShard) {
                long newGen = generation.incrementAndGet();
                transferred.set(true);
                log.debug("Two-phase transfer of {} shard {} to shard {}", leaseKey, sourceShard, destShard);
                return new LeaseEntry(destShard, newGen);
            }
            return existing;
        });
        return transferred.get();
    }

    public boolean rollback(String leaseKey, long targetShard) {
        String key = LEASE_PREFIX + leaseKey;
        LeaseEntry entry = leases.compute(key, (k, existing) -> {
            if (existing != null) {
                long newGen = generation.incrementAndGet();
                return new LeaseEntry(targetShard, newGen);
            }
            return existing;
        });
        return entry != null;
    }

    public boolean forceExpire(String leaseKey) {
        String key = LEASE_PREFIX + leaseKey;
        return leases.remove(key) != null;
    }

    public Set<String> getAllLeaseKeys() {
        return leases.keySet().stream()
            .filter(k -> k.startsWith(LEASE_PREFIX))
            .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Snapshot of all active leases for the control-plane lease table. Each entry carries
     * the key, holding shard, generation, and remaining ms until expiry (negative = past
     * expected duration, i.e. a transitional-state / duplication-precursor alert).
     */
    public java.util.List<Map<String, Object>> getLeaseSnapshot() {
        long now = System.currentTimeMillis();
        java.util.List<Map<String, Object>> out = new java.util.ArrayList<>();
        for (var e : leases.entrySet()) {
            LeaseEntry v = e.getValue();
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            String key = e.getKey();
            m.put("key", key.startsWith(LEASE_PREFIX) ? key.substring(LEASE_PREFIX.length()) : key);
            m.put("holder", v.shardId());
            m.put("generation", v.generation());
            m.put("expiresInMs", v.expiryTime() - now);
            m.put("transitional", v.expiryTime() - now < 0);
            out.add(m);
        }
        return out;
    }

    public boolean acquireSeat(String seatKey, long shardId, long timeoutMs) {
        String key = SEAT_PREFIX + seatKey;
        long newGen = generation.incrementAndGet();
        LeaseEntry entry = new LeaseEntry(shardId, newGen);
        entry.expiryTime = System.currentTimeMillis() + timeoutMs;
        LeaseEntry existing = leases.putIfAbsent(key, entry);
        return existing == null;
    }

    private boolean isExpired(LeaseEntry entry) {
        return entry != null && entry.expiryTime() <= System.currentTimeMillis();
    }

    private void startLeaseExpirationCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<String, LeaseEntry>> it = leases.entrySet().iterator();
            while (it.hasNext()) {
                var entry = it.next();
                if (entry.getValue().expiryTime() <= now) {
                    it.remove();
                    log.debug("Lease expired: {}", entry.getKey());
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    private static class LeaseEntry {
        private final long shardId;
        private final long generation;
        long expiryTime;

        LeaseEntry(long shardId, long generation) {
            this.shardId = shardId;
            this.generation = generation;
            this.expiryTime = System.currentTimeMillis() + DEFAULT_LEASE_TTL_MS;
        }

        public long shardId() { return shardId; }
        public long generation() { return generation; }
        public long expiryTime() { return expiryTime; }

        void renew() {
            this.expiryTime = System.currentTimeMillis() + DEFAULT_LEASE_TTL_MS;
        }
    }
}
