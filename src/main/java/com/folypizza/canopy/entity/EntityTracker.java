package com.folypizza.canopy.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EntityTracker {
    private static final Logger log = LoggerFactory.getLogger(EntityTracker.class);
    private static final int SEAM_THRESHOLD_BLOCKS = 8;

    public record EntityState(
        UUID entityId,
        double x, double y, double z,
        float yaw, float pitch,
        String entityType,
        long lastSeen,
        long owningShard
    ) {}

    private final ConcurrentHashMap<UUID, EntityState> entityStates = new ConcurrentHashMap<>();
    private final long localShardId;

    public EntityTracker(long localShardId) {
        this.localShardId = localShardId;
    }

    public void trackEntity(UUID entityId, double x, double y, double z,
                            float yaw, float pitch, String entityType) {
        var state = new EntityState(entityId, x, y, z, yaw, pitch, entityType, System.currentTimeMillis(), localShardId);
        entityStates.put(entityId, state);
    }

    public void untrackEntity(UUID entityId) {
        entityStates.remove(entityId);
    }

    public List<EntityState> queryNearby(double queryX, double queryZ, double radius) {
        double radiusSq = radius * radius;
        return entityStates.values().stream()
            .filter(s -> {
                var dx = s.x() - queryX;
                var dz = s.z() - queryZ;
                return (dx * dx + dz * dz) <= radiusSq;
            })
            .collect(Collectors.toList());
    }

    public boolean isNearSeam(UUID entityId, List<Long> seamXCoords, List<Long> seamZCoords) {
        var state = entityStates.get(entityId);
        if (state == null) return false;
        for (var sx : seamXCoords) {
            long dist = Math.abs(Math.round(state.x() - sx));
            if (dist <= SEAM_THRESHOLD_BLOCKS) return true;
        }
        for (var sz : seamZCoords) {
            long dist = Math.abs(Math.round(state.z() - sz));
            if (dist <= SEAM_THRESHOLD_BLOCKS) return true;
        }
        return false;
    }

    public void announcePosition(UUID entityId, double x, double y, double z, float yaw, float pitch) {
        var existing = entityStates.get(entityId);
        if (existing == null) {
            trackEntity(entityId, x, y, z, yaw, pitch, "unknown");
            return;
        }
        var updated = new EntityState(
            existing.entityId(), x, y, z, yaw, pitch,
            existing.entityType(), System.currentTimeMillis(), existing.owningShard()
        );
        entityStates.put(entityId, updated);
    }

    public EntityState getEntityState(UUID entityId) {
        var state = entityStates.get(entityId);
        if (state == null || System.currentTimeMillis() - state.lastSeen() > 30_000) {
            entityStates.remove(entityId);
            return null;
        }
        return state;
    }

    public int getTrackedCount() {
        return entityStates.size();
    }

    public Map<UUID, EntityState> getEntityStates() {
        return Collections.unmodifiableMap(entityStates);
    }

    public List<EntityState> getEntitiesForShard(long shardId) {
        return entityStates.values().stream()
            .filter(s -> s.owningShard() == shardId)
            .collect(Collectors.toList());
    }
}
