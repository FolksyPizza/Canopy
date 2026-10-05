package com.folksypizza.canopy.update;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Durable orchestration record for one idempotent zero-downtime update attempt. */
public record WorldUpdateState(UUID deploymentId, String worldId, String sourceInstance, String targetInstance,
                               long sourceEpoch, Phase phase, long revision, long checkpointSequence,
                               String checkpointSha256, long replayedSequence, long barrierSequence,
                               Set<Capability> capabilities, RecoveryMode recoveryMode) {
    public enum Phase {
        PREPARING, CATCHING_UP, FROZEN, READY, FENCED, PROMOTED, RECOVERING, ABORTED, COMPLETED
    }

    /** Attestations required before existing player sessions can be promoted. */
    public enum Capability {
        CRASH_CONSISTENT_WORLD_CHECKPOINT,
        COMPLETE_WORLD_MUTATION_JOURNAL,
        DETERMINISTIC_ORDERED_REPLAY,
        GLOBAL_AUTHORITY_FENCING,
        STANDBY_NON_AUTHORITATIVE,
        WORLD_SIMULATION_FROZEN,
        PLAYER_STATE_RESTORED,
        GATEWAY_SESSIONS_PRESERVED,
        CHAT_SCROLLBACK_PRESERVED,
        MAINTENANCE_NOTICE_APPENDED,
        RUNTIME_HEALTHY
    }

    public enum RecoveryMode { SOURCE_BEFORE_FENCE, TARGET_AFTER_FENCE }

    public static final Set<Capability> REQUIRED_PROMOTION_CAPABILITIES = Set.copyOf(EnumSet.allOf(Capability.class));

    public WorldUpdateState {
        Objects.requireNonNull(deploymentId, "deploymentId");
        requireName(worldId, "worldId");
        requireName(sourceInstance, "sourceInstance");
        requireName(targetInstance, "targetInstance");
        Objects.requireNonNull(phase, "phase");
        if (sourceInstance.equals(targetInstance)) throw new IllegalArgumentException("Update target is already authoritative");
        if (sourceEpoch < 1 || revision < 1 || checkpointSequence < 0 || replayedSequence < -1 || barrierSequence < -1) {
            throw new IllegalArgumentException("Invalid update counters");
        }
        if (checkpointSha256 != null && !checkpointSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Invalid checkpoint digest");
        }
        capabilities = Set.copyOf(Objects.requireNonNull(capabilities, "capabilities"));
        if ((phase == Phase.CATCHING_UP || phase == Phase.FROZEN || phase == Phase.READY || phase == Phase.FENCED
                || phase == Phase.PROMOTED || phase == Phase.COMPLETED)
                && (checkpointSha256 == null || !capabilities.contains(Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT))) {
            throw new IllegalArgumentException("Prepared update requires a crash-consistent checkpoint");
        }
        if ((phase == Phase.FROZEN || phase == Phase.READY || phase == Phase.FENCED || phase == Phase.PROMOTED
                || phase == Phase.COMPLETED) && barrierSequence < 0) {
            throw new IllegalArgumentException("Frozen update requires a final barrier");
        }
        if ((phase == Phase.READY || phase == Phase.FENCED || phase == Phase.PROMOTED || phase == Phase.COMPLETED)
                && replayedSequence != barrierSequence) {
            throw new IllegalArgumentException("Ready update must be replayed exactly through its final barrier");
        }
        if ((phase == Phase.RECOVERING) != (recoveryMode != null)) {
            throw new IllegalArgumentException("Recovery mode must match update phase");
        }
    }

    public boolean hasRequiredPromotionCapabilities() {
        return capabilities.containsAll(REQUIRED_PROMOTION_CAPABILITIES);
    }

    static long capabilityBits(Set<Capability> capabilities) {
        long bits = 0;
        for (Capability capability : capabilities) bits |= 1L << capability.ordinal();
        return bits;
    }

    static Set<Capability> capabilitiesFromBits(long bits) {
        long allowed = (1L << Capability.values().length) - 1;
        if ((bits & ~allowed) != 0) throw new IllegalArgumentException("Unknown capability bits");
        EnumSet<Capability> capabilities = EnumSet.noneOf(Capability.class);
        for (Capability capability : Capability.values()) {
            if ((bits & (1L << capability.ordinal())) != 0) capabilities.add(capability);
        }
        return Set.copyOf(capabilities);
    }

    private static void requireName(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.length() > 128) throw new IllegalArgumentException("Invalid " + field);
    }
}
