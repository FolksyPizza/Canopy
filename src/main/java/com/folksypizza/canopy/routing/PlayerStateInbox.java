package com.folksypizza.canopy.routing;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds player-state blobs pushed by a peer shard just before a proxy switch, keyed by
 * player UUID, until that player joins here and the state is applied.
 */
public class PlayerStateInbox {
    /** A state blob no real player comes close to; larger pushes are refused. */
    public static final int MAX_BLOB_BYTES = 1 << 20;
    /** Unclaimed states expire, so pushes for players who never arrive cannot pile up. */
    private static final long EXPIRE_MS = 30_000L;
    private static final int MAX_PENDING = 1024;

    private record Entry(byte[] blob, long at) {}

    private final ConcurrentHashMap<UUID, Entry> pending = new ConcurrentHashMap<>();

    public boolean put(UUID id, byte[] blob) {
        long now = System.currentTimeMillis();
        pending.values().removeIf(e -> now - e.at() > EXPIRE_MS);
        if (blob == null || blob.length > MAX_BLOB_BYTES || (!pending.containsKey(id) && pending.size() >= MAX_PENDING)) {
            return false;
        }
        pending.put(id, new Entry(blob, now));
        return true;
    }

    /** Retrieve and remove the pending blob for a player (null if none). */
    public byte[] take(UUID id) {
        Entry e = pending.remove(id);
        return e == null || System.currentTimeMillis() - e.at() > EXPIRE_MS ? null : e.blob();
    }

    public boolean has(UUID id) {
        return pending.containsKey(id);
    }
}
