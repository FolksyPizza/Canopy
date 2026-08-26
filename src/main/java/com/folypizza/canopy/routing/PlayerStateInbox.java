package com.folypizza.canopy.routing;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds player-state blobs pushed by a peer shard just before a proxy switch, keyed by
 * player UUID, until that player joins here and the state is applied.
 */
public class PlayerStateInbox {
    private final ConcurrentHashMap<UUID, byte[]> pending = new ConcurrentHashMap<>();

    public void put(UUID id, byte[] blob) {
        pending.put(id, blob);
    }

    /** Retrieve and remove the pending blob for a player (null if none). */
    public byte[] take(UUID id) {
        return pending.remove(id);
    }

    public boolean has(UUID id) {
        return pending.containsKey(id);
    }
}
