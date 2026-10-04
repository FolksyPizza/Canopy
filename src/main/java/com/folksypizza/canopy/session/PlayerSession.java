package com.folksypizza.canopy.session;

import java.util.Objects;
import java.util.UUID;

/** Durable session metadata. Backend attachment is independent of gateway presence. */
public record PlayerSession(UUID playerId, UUID sessionId, String gatewayId,
                            Phase phase, String owner, String target, UUID transferId,
                            long epoch, long revision, long snapshotSequence, byte[] snapshot,
                            long lifeRevision, Life life) {
    public static final int MAX_SNAPSHOT_BYTES = 1 << 20;
    public enum Phase { CONNECTED, ACTIVE, HANDOFF, CLOSING, OFFLINE }
    public enum Life { ALIVE, DEAD }

    public PlayerSession(UUID playerId, UUID sessionId, String gatewayId, Phase phase, String owner,
                         String target, UUID transferId, long epoch, long revision, long sequence, byte[] snapshot) {
        this(playerId, sessionId, gatewayId, phase, owner, target, transferId, epoch, revision, sequence,
            snapshot, 0, Life.ALIVE);
    }

    public PlayerSession {
        Objects.requireNonNull(playerId);
        Objects.requireNonNull(sessionId);
        requireName(gatewayId);
        Objects.requireNonNull(phase);
        Objects.requireNonNull(life);
        if (lifeRevision < 0) throw new IllegalArgumentException("Invalid life revision");
        if (owner != null) requireName(owner);
        if (target != null) requireName(target);
        if (epoch < 1 || revision < 1 || snapshotSequence < 0) {
            throw new IllegalArgumentException("Invalid session counters");
        }
        Objects.requireNonNull(snapshot);
        if (snapshot.length > MAX_SNAPSHOT_BYTES) throw new IllegalArgumentException("Snapshot too large");
        if ((phase == Phase.ACTIVE || phase == Phase.HANDOFF) && owner == null) {
            throw new IllegalArgumentException("Gameplay requires an owner");
        }
        if (phase == Phase.HANDOFF && (target == null || transferId == null || target.equals(owner))) {
            throw new IllegalArgumentException("Handoff requires a distinct target and transfer ID");
        }
        if (target != null && phase != Phase.HANDOFF && phase != Phase.CLOSING) {
            throw new IllegalArgumentException("Unexpected handoff target");
        }
        if ((phase == Phase.CONNECTED || phase == Phase.OFFLINE) && owner != null) {
            throw new IllegalArgumentException("Detached session has an owner");
        }
        snapshot = snapshot.clone();
    }

    @Override public byte[] snapshot() { return snapshot.clone(); }

    public int snapshotBytes() { return snapshot.length; }

    public boolean online() {
        return phase != Phase.CLOSING && phase != Phase.OFFLINE;
    }

    /** Never derives cluster presence from a backend Player.isOnline() value. */
    public GatewayToken gatewayToken() { return new GatewayToken(playerId, sessionId, gatewayId); }

    public AuthorityToken authorityToken() {
        if (owner == null) throw new IllegalStateException("Session has no gameplay owner");
        return new AuthorityToken(gatewayToken(), owner, epoch);
    }

    public record GatewayToken(UUID playerId, UUID sessionId, String gatewayId) {
        public GatewayToken {
            Objects.requireNonNull(playerId);
            Objects.requireNonNull(sessionId);
            requireName(gatewayId);
        }
    }

    public record AuthorityToken(GatewayToken gateway, String owner, long epoch) {
        public AuthorityToken {
            Objects.requireNonNull(gateway);
            requireName(owner);
            if (epoch < 1) throw new IllegalArgumentException("Invalid authority epoch");
        }
    }

    static void requireName(String name) {
        if (name == null || name.isBlank() || name.length() > 128) {
            throw new IllegalArgumentException("Invalid gateway or shard ID");
        }
    }
}
