package com.folksypizza.canopy.update;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static com.folksypizza.canopy.update.WorldUpdateState.Capability;
import static com.folksypizza.canopy.update.WorldUpdateState.Phase;
import static com.folksypizza.canopy.update.WorldUpdateState.RecoveryMode;

/**
 * Fail-closed state machine for a single world's zero-downtime update.
 * This class coordinates durable evidence; it does not claim to freeze Paper or produce a world journal.
 */
public final class ZeroDowntimeUpdateCoordinator {
    private static final Set<Capability> PRE_FREEZE_CAPABILITIES = Set.of(
        Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT,
        Capability.COMPLETE_WORLD_MUTATION_JOURNAL,
        Capability.DETERMINISTIC_ORDERED_REPLAY,
        Capability.GLOBAL_AUTHORITY_FENCING,
        Capability.STANDBY_NON_AUTHORITATIVE
    );

    private final WorldUpdateStore store;

    public ZeroDowntimeUpdateCoordinator(WorldUpdateStore store) {
        this.store = Objects.requireNonNull(store);
    }

    /** Begins an update using a caller-generated deployment ID that must be reused for retries. */
    public WorldUpdateState begin(UUID deploymentId, String worldId, String targetInstance) throws SQLException {
        Objects.requireNonNull(deploymentId, "deploymentId");
        WorldUpdateState existing = store.findUpdate(deploymentId).orElse(null);
        if (existing != null) {
            requireSamePlan(existing, worldId, targetInstance);
            return existing;
        }
        WorldAuthority authority = store.findAuthority(worldId)
            .orElseThrow(() -> new Conflict("World authority is not initialized"));
        if (authority.ownerInstance().equals(targetInstance)) throw new Conflict("Update target already owns this world");
        WorldUpdateState initial = new WorldUpdateState(deploymentId, worldId, authority.ownerInstance(), targetInstance,
            authority.epoch(), Phase.PREPARING, 1, 0, null, -1, -1, Set.of(), null);
        if (store.beginIfAuthority(initial)) return initial;
        existing = store.findUpdate(deploymentId).orElse(null);
        if (existing != null) {
            requireSamePlan(existing, worldId, targetInstance);
            return existing;
        }
        throw new Conflict("World authority changed while update was starting");
    }

    /** Records the standby's immutable, crash-consistent checkpoint manifest. */
    public WorldUpdateState beginCatchUp(UUID deploymentId, long checkpointSequence, String checkpointSha256,
                                         Set<Capability> capabilities) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        requireCapability(capabilities, Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT);
        if (checkpointSequence < 0 || checkpointSha256 == null || !checkpointSha256.matches("[0-9a-f]{64}")) {
            throw new Conflict("Checkpoint manifest is incomplete");
        }
        if (state.phase() == Phase.CATCHING_UP && state.checkpointSequence() == checkpointSequence
                && checkpointSha256.equals(state.checkpointSha256()) && state.capabilities().equals(Set.copyOf(capabilities))) {
            return state;
        }
        requirePhase(state, Phase.PREPARING);
        WorldUpdateState next = copy(state, Phase.CATCHING_UP, state.revision() + 1, checkpointSequence,
            checkpointSha256, -1, -1, capabilities, null);
        return save(state, next);
    }

    /** Reports the contiguous, ordered journal watermark applied by the non-authoritative standby. */
    public WorldUpdateState reportCatchUp(UUID deploymentId, long appliedSequence,
                                          Set<Capability> capabilities) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        if ((state.phase() == Phase.CATCHING_UP || state.phase() == Phase.FROZEN)
                && appliedSequence == state.replayedSequence()
                && state.capabilities().equals(Set.copyOf(capabilities))) return state;
        if (state.phase() != Phase.CATCHING_UP && state.phase() != Phase.FROZEN) {
            throw new Conflict("Update is not catching up");
        }
        if (!capabilities.contains(Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT)) {
            throw new Conflict("Replay evidence lost its checkpoint capability");
        }
        if (appliedSequence < state.checkpointSequence() || appliedSequence < state.replayedSequence()) {
            throw new Conflict("Replay watermark moved backwards");
        }
        if (state.phase() == Phase.FROZEN && appliedSequence > state.barrierSequence()) {
            throw new Conflict("Replay advanced beyond the frozen barrier");
        }
        WorldUpdateState next = copy(state, state.phase(), state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), appliedSequence, state.barrierSequence(), capabilities, null);
        return save(state, next);
    }

    /**
     * Records the final source barrier. The caller must already have drained gateway input, frozen players and world
     * simulation, and appended the maintenance notice without clearing chat.
     */
    public WorldUpdateState freezeAtBarrier(UUID deploymentId, FreezeEvidence evidence) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        verifySourceEvidence(state, evidence.deploymentId(), evidence.worldId(), evidence.sourceInstance());
        if (state.phase() == Phase.FROZEN && state.barrierSequence() == evidence.barrierSequence()) return state;
        requirePhase(state, Phase.CATCHING_UP);
        if (!evidence.clientInputsDrained() || !evidence.playersFrozen() || !evidence.worldSimulationFrozen()
                || !evidence.noticeAppended()) {
            throw new Conflict("Freeze barrier is incomplete");
        }
        if (!state.capabilities().containsAll(PRE_FREEZE_CAPABILITIES)) {
            throw new Conflict("Complete world journaling and ordered replay are required before freezing players");
        }
        if (evidence.barrierSequence() < state.replayedSequence()
                || evidence.barrierSequence() < state.checkpointSequence()) {
            throw new Conflict("Freeze barrier precedes a durable checkpoint or applied journal entry");
        }
        var capabilities = new java.util.HashSet<>(state.capabilities());
        capabilities.add(Capability.WORLD_SIMULATION_FROZEN);
        capabilities.add(Capability.MAINTENANCE_NOTICE_APPENDED);
        WorldUpdateState next = copy(state, Phase.FROZEN, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), evidence.barrierSequence(), capabilities, null);
        return save(state, next);
    }

    /** Stores a destination readiness receipt only when it exactly covers the frozen barrier. */
    public WorldUpdateState markReady(UUID deploymentId, ReadinessEvidence evidence) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        verifyTargetEvidence(state, evidence.deploymentId(), evidence.worldId(), evidence.targetInstance());
        if (state.phase() == Phase.READY && readinessMatches(state, evidence)) return state;
        requirePhase(state, Phase.FROZEN);
        if (evidence.checkpointSequence() != state.checkpointSequence()
                || !Objects.equals(evidence.checkpointSha256(), state.checkpointSha256())
                || evidence.appliedSequence() != state.barrierSequence()
                || state.replayedSequence() != state.barrierSequence()) {
            throw new Conflict("Standby has not applied the exact checkpoint and final barrier");
        }
        if (!evidence.capabilities().containsAll(WorldUpdateState.REQUIRED_PROMOTION_CAPABILITIES)) {
            throw new Conflict("Standby readiness lacks required world, session or chat continuity capability");
        }
        WorldUpdateState next = copy(state, Phase.READY, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), evidence.capabilities(), null);
        return save(state, next);
    }

    /** Atomically advances the durable world owner and epoch; the old epoch must never be reactivated afterward. */
    public WorldUpdateState fenceAuthority(UUID deploymentId) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        if (state.phase() == Phase.FENCED || state.phase() == Phase.PROMOTED || state.phase() == Phase.COMPLETED) {
            verifyTargetAuthority(state);
            return state;
        }
        requirePhase(state, Phase.READY);
        if (!state.hasRequiredPromotionCapabilities()) throw new Conflict("Promotion capabilities are incomplete");
        if (state.sourceEpoch() == Long.MAX_VALUE) throw new Conflict("World authority epoch is exhausted");
        WorldUpdateState next = copy(state, Phase.FENCED, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), state.capabilities(), null);
        if (!store.fenceAuthority(state, next)) throw new Conflict("World authority or update revision was superseded");
        return next;
    }

    /** Requires the target to acknowledge the new epoch and every frozen live session before promotion. */
    public WorldUpdateState promote(UUID deploymentId, PromotionEvidence evidence) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        verifyTargetEvidence(state, evidence.deploymentId(), evidence.worldId(), evidence.targetInstance());
        if (state.phase() == Phase.PROMOTED || state.phase() == Phase.COMPLETED) {
            verifyTargetAuthority(state);
            return state;
        }
        requirePhase(state, Phase.FENCED);
        verifyTargetEpoch(state, evidence.authorityEpoch());
        if (!evidence.targetTickConfirmed() || !evidence.liveSessionsRouted() || !evidence.sessionsReady()) {
            throw new Conflict("Target world or live sessions are not ready for promotion");
        }
        WorldUpdateState next = copy(state, Phase.PROMOTED, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), state.capabilities(), null);
        return save(state, next);
    }

    /** Completes only after gameplay and audit checks pass and all sessions have been safely released. */
    public WorldUpdateState complete(UUID deploymentId, CompletionEvidence evidence) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        verifyTargetEvidence(state, evidence.deploymentId(), evidence.worldId(), evidence.targetInstance());
        if (state.phase() == Phase.COMPLETED) {
            verifyTargetAuthority(state);
            return state;
        }
        requirePhase(state, Phase.PROMOTED);
        verifyTargetEpoch(state, evidence.authorityEpoch());
        if (!evidence.movementVerified() || !evidence.savesVerified() || !evidence.auditContinuous()
                || !evidence.sessionsUnfrozen()) {
            throw new Conflict("Post-promotion verification is incomplete");
        }
        WorldUpdateState next = copy(state, Phase.COMPLETED, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), state.capabilities(), null);
        return save(state, next);
    }

    /** Starts either pre-fence source recovery or post-fence forward recovery; it never rewinds world authority. */
    public WorldUpdateState recover(UUID deploymentId) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        if (state.phase() == Phase.ABORTED || state.phase() == Phase.COMPLETED) return state;
        if (state.phase() == Phase.RECOVERING) return state;
        WorldAuthority authority = store.findAuthority(state.worldId())
            .orElseThrow(() -> new Conflict("World authority is unavailable during recovery"));
        RecoveryMode mode;
        if (authority.ownerInstance().equals(state.sourceInstance()) && authority.epoch() == state.sourceEpoch()) {
            if (state.phase() == Phase.FENCED || state.phase() == Phase.PROMOTED) {
                throw new Conflict("Fenced update lost its target authority record");
            }
            mode = RecoveryMode.SOURCE_BEFORE_FENCE;
        } else if (authority.ownerInstance().equals(state.targetInstance()) && authority.epoch() == state.sourceEpoch() + 1) {
            if (state.phase() != Phase.FENCED && state.phase() != Phase.PROMOTED) {
                throw new Conflict("World authority advanced before the recorded fence");
            }
            mode = RecoveryMode.TARGET_AFTER_FENCE;
        } else {
            throw new Conflict("World authority is held by an unexpected owner or epoch");
        }
        WorldUpdateState next = copy(state, Phase.RECOVERING, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), state.capabilities(), mode);
        return save(state, next);
    }

    /** Releases a pre-fence recovery only after confirming the original source still owns the unchanged epoch. */
    public WorldUpdateState finishRecoveryOnSource(UUID deploymentId) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        requireRecoveryMode(state, RecoveryMode.SOURCE_BEFORE_FENCE);
        verifySourceAuthority(state);
        WorldUpdateState next = copy(state, Phase.ABORTED, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), state.capabilities(), null);
        return save(state, next);
    }

    /**
     * Finishes post-fence recovery on the new owner. There is intentionally no method that restores the stale source
     * epoch; another update must create a new, higher-epoch deployment if the source needs to return.
     */
    public WorldUpdateState finishRecoveryOnTarget(UUID deploymentId, RecoveryEvidence evidence) throws SQLException {
        WorldUpdateState state = required(deploymentId);
        verifyTargetEvidence(state, evidence.deploymentId(), evidence.worldId(), evidence.targetInstance());
        requireRecoveryMode(state, RecoveryMode.TARGET_AFTER_FENCE);
        verifyTargetEpoch(state, evidence.authorityEpoch());
        if (!evidence.targetHealthy() || !evidence.sessionsRecovered() || !evidence.auditContinuous()) {
            throw new Conflict("Forward recovery is not ready to release players");
        }
        WorldUpdateState next = copy(state, Phase.COMPLETED, state.revision() + 1, state.checkpointSequence(),
            state.checkpointSha256(), state.replayedSequence(), state.barrierSequence(), state.capabilities(), null);
        return save(state, next);
    }

    private WorldUpdateState required(UUID deploymentId) throws SQLException {
        return store.findUpdate(deploymentId).orElseThrow(() -> new Conflict("Unknown deployment ID"));
    }

    private WorldUpdateState save(WorldUpdateState expected, WorldUpdateState next) throws SQLException {
        if (!store.compareAndSet(expected.revision(), next)) throw new Conflict("Update revision was superseded");
        return next;
    }

    private void verifySourceAuthority(WorldUpdateState state) throws SQLException {
        WorldAuthority authority = store.findAuthority(state.worldId())
            .orElseThrow(() -> new Conflict("World authority is unavailable"));
        if (!authority.ownerInstance().equals(state.sourceInstance()) || authority.epoch() != state.sourceEpoch()) {
            throw new Conflict("Original source no longer owns the world");
        }
    }

    private void verifyTargetAuthority(WorldUpdateState state) throws SQLException {
        WorldAuthority authority = store.findAuthority(state.worldId())
            .orElseThrow(() -> new Conflict("World authority is unavailable"));
        if (!authority.ownerInstance().equals(state.targetInstance()) || authority.epoch() != state.sourceEpoch() + 1
                || !state.deploymentId().equals(authority.deploymentId())) {
            throw new Conflict("Target authority does not match the fenced deployment");
        }
    }

    private void verifyTargetEpoch(WorldUpdateState state, long epoch) throws SQLException {
        verifyTargetAuthority(state);
        if (epoch != state.sourceEpoch() + 1) throw new Conflict("Target acknowledged a stale authority epoch");
    }

    private static void requirePhase(WorldUpdateState state, Phase phase) {
        if (state.phase() != phase) throw new Conflict("Expected update phase " + phase);
    }

    private static void requireRecoveryMode(WorldUpdateState state, RecoveryMode mode) {
        requirePhase(state, Phase.RECOVERING);
        if (state.recoveryMode() != mode) throw new Conflict("Recovery path does not match the authority fence");
    }

    private static void requireCapability(Set<Capability> capabilities, Capability capability) {
        if (capabilities == null || !capabilities.contains(capability)) {
            throw new Conflict("Missing readiness capability: " + capability);
        }
    }

    private static void requireSamePlan(WorldUpdateState state, String worldId, String targetInstance) {
        if (!state.worldId().equals(worldId) || !state.targetInstance().equals(targetInstance)) {
            throw new Conflict("Deployment ID was already used for a different update");
        }
    }

    private static void verifySourceEvidence(WorldUpdateState state, UUID deploymentId, String worldId, String source) {
        verifyIdentity(state, deploymentId, worldId);
        if (!state.sourceInstance().equals(source)) throw new Conflict("Evidence came from the wrong source instance");
    }

    private static void verifyTargetEvidence(WorldUpdateState state, UUID deploymentId, String worldId, String target) {
        verifyIdentity(state, deploymentId, worldId);
        if (!state.targetInstance().equals(target)) throw new Conflict("Evidence came from the wrong target instance");
    }

    private static void verifyIdentity(WorldUpdateState state, UUID deploymentId, String worldId) {
        if (!state.deploymentId().equals(deploymentId) || !state.worldId().equals(worldId)) {
            throw new Conflict("Evidence belongs to another deployment or world");
        }
    }

    private static boolean readinessMatches(WorldUpdateState state, ReadinessEvidence evidence) {
        return state.checkpointSequence() == evidence.checkpointSequence()
            && Objects.equals(state.checkpointSha256(), evidence.checkpointSha256())
            && state.barrierSequence() == evidence.appliedSequence()
            && state.capabilities().equals(evidence.capabilities());
    }

    private static WorldUpdateState copy(WorldUpdateState state, Phase phase, long revision, long checkpointSequence,
                                         String checkpointSha256, long replayedSequence, long barrierSequence,
                                         Set<Capability> capabilities, RecoveryMode recoveryMode) {
        return new WorldUpdateState(state.deploymentId(), state.worldId(), state.sourceInstance(), state.targetInstance(),
            state.sourceEpoch(), phase, revision, checkpointSequence, checkpointSha256, replayedSequence,
            barrierSequence, capabilities, recoveryMode);
    }

    public record FreezeEvidence(UUID deploymentId, String worldId, String sourceInstance, long barrierSequence,
                                 boolean clientInputsDrained, boolean playersFrozen,
                                 boolean worldSimulationFrozen, boolean noticeAppended) {
        public FreezeEvidence {
            Objects.requireNonNull(deploymentId); Objects.requireNonNull(worldId); Objects.requireNonNull(sourceInstance);
        }
    }

    /**
     * Capability claims must come from authenticated checkpoint/journal, Paper lifecycle and gateway providers.
     * The current Canopy serializers and halo trackers are not valid providers for complete world-state evidence.
     */
    public record ReadinessEvidence(UUID deploymentId, String worldId, String targetInstance,
                                    long checkpointSequence, String checkpointSha256, long appliedSequence,
                                    Set<Capability> capabilities) {
        public ReadinessEvidence {
            Objects.requireNonNull(deploymentId); Objects.requireNonNull(worldId); Objects.requireNonNull(targetInstance);
            capabilities = Set.copyOf(Objects.requireNonNull(capabilities));
        }
    }

    public record PromotionEvidence(UUID deploymentId, String worldId, String targetInstance, long authorityEpoch,
                                    boolean targetTickConfirmed, boolean liveSessionsRouted, boolean sessionsReady) {
        public PromotionEvidence {
            Objects.requireNonNull(deploymentId); Objects.requireNonNull(worldId); Objects.requireNonNull(targetInstance);
        }
    }

    public record CompletionEvidence(UUID deploymentId, String worldId, String targetInstance, long authorityEpoch,
                                     boolean movementVerified, boolean savesVerified, boolean auditContinuous,
                                     boolean sessionsUnfrozen) {
        public CompletionEvidence {
            Objects.requireNonNull(deploymentId); Objects.requireNonNull(worldId); Objects.requireNonNull(targetInstance);
        }
    }

    public record RecoveryEvidence(UUID deploymentId, String worldId, String targetInstance, long authorityEpoch,
                                   boolean targetHealthy, boolean sessionsRecovered, boolean auditContinuous) {
        public RecoveryEvidence {
            Objects.requireNonNull(deploymentId); Objects.requireNonNull(worldId); Objects.requireNonNull(targetInstance);
        }
    }

    public static final class Conflict extends IllegalStateException {
        public Conflict(String message) { super(message); }
    }
}
