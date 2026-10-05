package com.folksypizza.canopy.update;

import io.papermc.paper.update.WorldAuthorityControl;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class WorldAuthorityBootstrapTest {
    private static final String WORLD = "world-main";
    private static final String INSTANCE = "paper-main-a";
    private static final long EPOCH = 12;

    @Test void missingPaperBridgeFailsBeforeOpeningGate() throws Exception {
        var control = new FakeControl();
        var store = storeWithInitialAuthority();
        WorldAuthorityControl.AuthorityProof proof = WorldAuthorityBootstrap.prepare(WORLD, store);

        assertThrows(NullPointerException.class,
            () -> WorldAuthorityBootstrap.open(WORLD, proof, null, store));

        assertNull(control.openedProof);
        assertEquals(WorldAuthorityControl.Phase.STANDBY, control.snapshot.phase());
    }

    @Test void unsupportedApiVersionIsRejectedWithoutOpeningPaperGate() throws Exception {
        var control = new FakeControl();
        control.apiVersion = WorldAuthorityControl.API_VERSION + 1;
        var store = storeWithInitialAuthority();
        WorldAuthorityControl.AuthorityProof proof = WorldAuthorityBootstrap.prepare(WORLD, store);

        assertThrows(IllegalStateException.class, () -> openPrepared(WORLD, proof, store, control));

        assertNull(control.openedProof);
        assertEquals(WorldAuthorityControl.Phase.STANDBY, control.snapshot.phase());
    }

    @Test void databaseReadFailureKeepsPaperClosedAndPropagates() throws Exception {
        var control = new FakeControl();
        var store = new MemoryStore();
        store.activeUpdateFailure = new SQLException("database unavailable");

        assertThrows(SQLException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));

        assertNull(control.openedProof);
        assertEquals(0, store.authorityReads);
    }

    @Test void missingDurableAuthorityFailsClosedWithoutSeeding() {
        var control = new FakeControl();
        var store = new MemoryStore();

        assertThrows(IllegalStateException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));

        assertEquals(0, store.initializeCalls);
        assertNull(control.openedProof);
    }

    @Test void unresolvedActiveUpdatePreventsAuthorityOpening() {
        var control = new FakeControl();
        var store = storeWithInitialAuthority();
        store.active = Optional.of(new WorldUpdateState(UUID.randomUUID(), WORLD, INSTANCE,
            "paper-standby-b", EPOCH, WorldUpdateState.Phase.PREPARING, 1,
            0, null, -1, -1, Set.of(), null));

        assertThrows(IllegalStateException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));

        assertEquals(1, store.activeUpdateReads);
        assertEquals(0, store.authorityReads);
        assertNull(control.openedProof);
    }

    @Test void configuredWorldMustMatchPaperSnapshot() throws Exception {
        var control = new FakeControl();
        control.snapshot = snapshot("other-world", INSTANCE, EPOCH);
        var store = storeWithInitialAuthority();
        WorldAuthorityControl.AuthorityProof proof = WorldAuthorityBootstrap.prepare(WORLD, store);

        assertThrows(IllegalStateException.class, () -> openPrepared(WORLD, proof, store, control));

        assertNotNull(control.openedProof);
        assertEquals(WorldAuthorityControl.Phase.STANDBY, control.snapshot.phase());
    }

    @Test void onlyExactLocalOwnerAndEpochCanReturnActive() throws Exception {
        var wrongOwner = new FakeControl();
        wrongOwner.snapshot = snapshot(WORLD, "paper-other", EPOCH);
        assertThrows(IllegalStateException.class, () -> open(WORLD, storeWithInitialAuthority(), wrongOwner));
        assertEquals(WorldAuthorityControl.Phase.STANDBY, wrongOwner.snapshot.phase());

        var wrongEpoch = new FakeControl();
        wrongEpoch.snapshot = snapshot(WORLD, INSTANCE, EPOCH + 2);
        assertThrows(IllegalStateException.class, () -> open(WORLD, storeWithInitialAuthority(), wrongEpoch));
        assertEquals(WorldAuthorityControl.Phase.STANDBY, wrongEpoch.snapshot.phase());
    }

    @Test void configuredWorldMustMatchDurableAuthority() {
        var store = new MemoryStore();
        store.authority = Optional.of(new WorldAuthority("other-world", INSTANCE, EPOCH, null));

        assertThrows(IllegalStateException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));
    }

    @Test void danglingTransitionOwnedAuthorityFailsClosed() {
        var store = storeWithCompletedAuthority();
        store.updates.clear();

        assertThrows(IllegalStateException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));
    }

    @Test void receiptReadFailureKeepsBootstrapClosed() {
        var store = storeWithCompletedAuthority();
        store.updateFailure = new SQLException("receipt read failed");

        assertThrows(SQLException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));
    }

    @Test void nonterminalTransitionCannotBootstrapCurrentOwner() {
        var store = storeWithCompletedAuthority();
        UUID deployment = store.authority.orElseThrow().deploymentId();
        store.updates.put(deployment, update(deployment, WORLD, "paper-old", INSTANCE, EPOCH - 1,
            WorldUpdateState.Phase.PREPARING, Set.of()));

        assertThrows(IllegalStateException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));
    }

    @Test void mismatchedCompletedTransitionCannotBootstrapCurrentOwner() {
        var store = storeWithCompletedAuthority();
        UUID deployment = store.authority.orElseThrow().deploymentId();
        store.updates.put(deployment,
            completedUpdate(deployment, WORLD, "paper-old", "paper-other", EPOCH - 1));

        assertThrows(IllegalStateException.class, () -> WorldAuthorityBootstrap.prepare(WORLD, store));
    }

    @Test void completedTransitionMustMatchAuthorityDeploymentAndCapabilities() {
        var mismatchedDeployment = storeWithCompletedAuthority();
        UUID deployment = mismatchedDeployment.authority.orElseThrow().deploymentId();
        UUID otherDeployment = UUID.randomUUID();
        mismatchedDeployment.updates.put(deployment,
            completedUpdate(otherDeployment, WORLD, "paper-old", INSTANCE, EPOCH - 1));
        assertThrows(IllegalStateException.class,
            () -> WorldAuthorityBootstrap.prepare(WORLD, mismatchedDeployment));

        var missingCapability = storeWithCompletedAuthority();
        UUID requiredDeployment = missingCapability.authority.orElseThrow().deploymentId();
        Set<WorldUpdateState.Capability> incomplete = new java.util.HashSet<>(
            WorldUpdateState.REQUIRED_PROMOTION_CAPABILITIES);
        incomplete.remove(WorldUpdateState.Capability.COMPLETE_WORLD_MUTATION_JOURNAL);
        missingCapability.updates.put(requiredDeployment,
            completedUpdate(requiredDeployment, WORLD, "paper-old", INSTANCE, EPOCH - 1, incomplete));
        assertThrows(IllegalStateException.class,
            () -> WorldAuthorityBootstrap.prepare(WORLD, missingCapability));
    }

    @Test void durableAuthorityChangesDuringStartupKeepGateClosed() throws Exception {
        var control = new FakeControl();
        var store = storeWithInitialAuthority();
        WorldAuthorityControl.AuthorityProof proof = WorldAuthorityBootstrap.prepare(WORLD, store);
        store.authority = Optional.of(new WorldAuthority(WORLD, "paper-other", EPOCH + 1, UUID.randomUUID()));

        assertThrows(IllegalStateException.class, () -> openPrepared(WORLD, proof, store, control));

        assertNull(control.openedProof);
    }

    @Test void completedDeploymentAuthorityCanRecoverExactOwnerAfterRestart() throws Exception {
        var control = new FakeControl();
        var store = storeWithCompletedAuthority();
        WorldAuthorityControl.AuthorityProof proof = WorldAuthorityBootstrap.prepare(WORLD, store);

        WorldAuthorityBootstrap.Result result = openPrepared(WORLD, proof, store, control);

        assertEquals(new WorldAuthorityControl.AuthorityProof(
            WORLD, INSTANCE, EPOCH, store.authority.orElseThrow().deploymentId()), proof);
        assertEquals(proof, control.openedProof);
        assertEquals(WorldAuthorityControl.Phase.ACTIVE, result.phase());
        assertSame(control, result.control());
        assertEquals(4, store.activeUpdateReads);
        assertEquals(4, store.authorityReads);
        assertEquals(4, store.updateReads);
    }

    @Test void prebootedGhostReturnsClosedStandbyAtNextEpoch() throws Exception {
        var control = new FakeControl();
        control.snapshot = snapshot(WORLD, "paper-standby-b", EPOCH + 1);
        var store = storeWithInitialAuthority();

        WorldAuthorityBootstrap.Result result = open(WORLD, store, control);

        assertEquals(WorldAuthorityControl.Phase.STANDBY, result.phase());
        assertEquals(WorldAuthorityControl.Phase.STANDBY, result.snapshot().phase());
        assertNotNull(result.control());
        assertEquals(new WorldAuthorityControl.AuthorityProof(WORLD, INSTANCE, EPOCH, null), control.openedProof);
    }

    @Test void ghostMayUseExactCompletedAuthorityReceiptAndStaysClosed() throws Exception {
        var control = new FakeControl();
        control.snapshot = snapshot(WORLD, "paper-standby-b", EPOCH + 1);
        var store = storeWithCompletedAuthority();

        WorldAuthorityBootstrap.Result result = open(WORLD, store, control);

        assertEquals(WorldAuthorityControl.Phase.STANDBY, result.phase());
        assertEquals(store.authority.orElseThrow().deploymentId(), control.openedProof.deploymentId());
    }

    @Test void initialAuthorityOpensWithNullDeploymentProof() throws Exception {
        var control = new FakeControl();
        var store = storeWithInitialAuthority();

        WorldAuthorityBootstrap.Result result = open(WORLD, store, control);

        assertEquals(new WorldAuthorityControl.AuthorityProof(WORLD, INSTANCE, EPOCH, null), control.openedProof);
        assertEquals(WorldAuthorityControl.Phase.ACTIVE, result.phase());
        assertEquals(4, store.activeUpdateReads);
        assertEquals(4, store.authorityReads);
        assertEquals(0, store.updateReads);
        assertEquals(0, store.initializeCalls);
    }

    private static MemoryStore storeWithInitialAuthority() {
        var store = new MemoryStore();
        store.authority = Optional.of(new WorldAuthority(WORLD, INSTANCE, EPOCH, null));
        return store;
    }

    private static MemoryStore storeWithCompletedAuthority() {
        var store = new MemoryStore();
        UUID deployment = UUID.randomUUID();
        store.authority = Optional.of(new WorldAuthority(WORLD, INSTANCE, EPOCH, deployment));
        store.updates.put(deployment, completedUpdate(deployment, WORLD, "paper-old", INSTANCE, EPOCH - 1));
        return store;
    }

    private static WorldUpdateState completedUpdate(
            UUID deployment, String world, String source, String target, long epoch) {
        return completedUpdate(deployment, world, source, target, epoch,
            WorldUpdateState.REQUIRED_PROMOTION_CAPABILITIES);
    }

    private static WorldUpdateState completedUpdate(
            UUID deployment, String world, String source, String target, long epoch,
            Set<WorldUpdateState.Capability> capabilities) {
        return update(deployment, world, source, target, epoch, WorldUpdateState.Phase.COMPLETED,
            capabilities);
    }

    private static WorldUpdateState update(UUID deployment, String world, String source, String target, long epoch,
                                           WorldUpdateState.Phase phase, Set<WorldUpdateState.Capability> capabilities) {
        boolean completed = phase == WorldUpdateState.Phase.COMPLETED;
        return new WorldUpdateState(deployment, world, source, target, epoch, phase,
            completed ? 8 : 1, completed ? 100 : 0, completed ? "a".repeat(64) : null,
            completed ? 100 : -1, completed ? 100 : -1, capabilities, null);
    }

    private static WorldAuthorityBootstrap.Result open(String worldId, MemoryStore store, FakeControl control)
            throws SQLException {
        WorldAuthorityControl.AuthorityProof proof = WorldAuthorityBootstrap.prepare(worldId, store);
        return openPrepared(worldId, proof, store, control);
    }

    private static WorldAuthorityBootstrap.Result openPrepared(
            String worldId, WorldAuthorityControl.AuthorityProof proof, MemoryStore store, FakeControl control)
            throws SQLException {
        return WorldAuthorityBootstrap.open(worldId, proof,
            candidate -> control.openForProof(WorldAuthorityControl.API_VERSION, candidate), store);
    }

    private static WorldAuthorityControl.Snapshot snapshot(String world, String instance, long epoch) {
        return new WorldAuthorityControl.Snapshot(world, instance, epoch,
            WorldAuthorityControl.Phase.STANDBY, null, null, 0);
    }

    private static final class FakeControl implements WorldAuthorityControl {
        private int apiVersion = API_VERSION;
        private Snapshot snapshot = WorldAuthorityBootstrapTest.snapshot(WORLD, INSTANCE, EPOCH);
        private AuthorityProof openedProof;

        @Override public int apiVersion() { return apiVersion; }
        @Override public Snapshot snapshot() { return snapshot; }
        @Override public CompletionStage<Snapshot> quiesce(UUID transitionId, long expectedEpoch) {
            return CompletableFuture.completedFuture(snapshot);
        }
        @Override public boolean resume(UUID transitionId) { return false; }
        @Override public boolean preparePromotion(UUID transitionId, String targetInstance) { return false; }
        @Override public boolean abortPromotionAfterSourceAuthorityConfirmed(UUID transitionId, AuthorityProof proof) {
            return false;
        }
        @Override public boolean retire(UUID transitionId, AuthorityProof proof) { return false; }
        @Override public boolean activate(UUID transitionId, AuthorityProof proof) { return false; }

        private WorldAuthorityControl openForProof(int expectedApiVersion, AuthorityProof proof) {
            if (expectedApiVersion != apiVersion) return this;
            openedProof = proof;
            if (proof.worldId().equals(snapshot.worldId()) && proof.ownerInstance().equals(snapshot.instanceId())
                    && proof.epoch() == snapshot.epoch()) {
                snapshot = new Snapshot(snapshot.worldId(), snapshot.instanceId(), snapshot.epoch(), Phase.ACTIVE,
                    null, null, 0);
            }
            return this;
        }
    }

    private static final class MemoryStore implements WorldUpdateStore {
        private Optional<WorldAuthority> authority = Optional.empty();
        private Optional<WorldUpdateState> active = Optional.empty();
        private final Map<UUID, WorldUpdateState> updates = new HashMap<>();
        private SQLException activeUpdateFailure;
        private SQLException updateFailure;
        private int authorityReads;
        private int activeUpdateReads;
        private int updateReads;
        private int initializeCalls;

        @Override public Optional<WorldAuthority> findAuthority(String worldId) {
            authorityReads++;
            return authority;
        }
        @Override public boolean initializeAuthority(WorldAuthority initial) {
            initializeCalls++;
            return false;
        }
        @Override public Optional<WorldUpdateState> findUpdate(UUID deploymentId) throws SQLException {
            updateReads++;
            if (updateFailure != null) throw updateFailure;
            return Optional.ofNullable(updates.get(deploymentId));
        }
        @Override public Optional<WorldUpdateState> findActiveUpdate(String worldId) throws SQLException {
            activeUpdateReads++;
            if (activeUpdateFailure != null) throw activeUpdateFailure;
            return active;
        }
        @Override public boolean beginIfAuthority(WorldUpdateState initial) { return false; }
        @Override public boolean compareAndSet(long expectedRevision, WorldUpdateState next) { return false; }
        @Override public boolean fenceAuthority(WorldUpdateState expectedReady, WorldUpdateState fenced) { return false; }
    }
}
