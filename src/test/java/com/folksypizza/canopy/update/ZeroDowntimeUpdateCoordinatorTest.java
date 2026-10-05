package com.folksypizza.canopy.update;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.folksypizza.canopy.update.WorldUpdateState.Capability;
import static com.folksypizza.canopy.update.WorldUpdateState.Phase;
import static com.folksypizza.canopy.update.WorldUpdateState.RecoveryMode;
import static org.junit.jupiter.api.Assertions.*;

class ZeroDowntimeUpdateCoordinatorTest {
    private static final String WORLD = "world-main";
    private static final String SOURCE = "paper-main-a";
    private static final String TARGET = "paper-standby-b";
    private static final String DIGEST = "a".repeat(64);

    @Test void completeUpdateFencesExactlyOnceAndRequiresAllContinuityEvidence() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 12, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID deployment = UUID.randomUUID();

        WorldUpdateState preparing = coordinator.begin(deployment, WORLD, TARGET);
        assertEquals(Phase.PREPARING, preparing.phase());
        assertEquals(preparing, coordinator.begin(deployment, WORLD, TARGET));
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.begin(deployment, "another-world", TARGET));

        WorldUpdateState catchingUp = coordinator.beginCatchUp(deployment, 100, DIGEST, preFreezeCapabilities());
        assertEquals(Phase.CATCHING_UP, catchingUp.phase());
        coordinator.reportCatchUp(deployment, 104, preFreezeCapabilities());

        var freeze = new ZeroDowntimeUpdateCoordinator.FreezeEvidence(deployment, WORLD, SOURCE, 108,
            true, true, true, true);
        WorldUpdateState frozen = coordinator.freezeAtBarrier(deployment, freeze);
        assertEquals(Phase.FROZEN, frozen.phase());
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.markReady(deployment, readiness(deployment, 104, fullCapabilities())));
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.markReady(deployment, new ZeroDowntimeUpdateCoordinator.ReadinessEvidence(
                deployment, WORLD, SOURCE, 100, DIGEST, 104, fullCapabilities())));

        coordinator.reportCatchUp(deployment, 108, fullCapabilities());
        WorldUpdateState ready = coordinator.markReady(deployment, readiness(deployment, 108, fullCapabilities()));
        assertEquals(Phase.READY, ready.phase());
        assertTrue(ready.hasRequiredPromotionCapabilities());

        WorldUpdateState fenced = coordinator.fenceAuthority(deployment);
        assertEquals(Phase.FENCED, fenced.phase());
        assertEquals(new WorldAuthority(WORLD, TARGET, 13, deployment), store.findAuthority(WORLD).orElseThrow());
        assertEquals(fenced, coordinator.fenceAuthority(deployment));
        assertEquals(13, store.findAuthority(WORLD).orElseThrow().epoch());

        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.promote(deployment, new ZeroDowntimeUpdateCoordinator.PromotionEvidence(
                deployment, WORLD, TARGET, 13, true, true, false)));
        WorldUpdateState promoted = coordinator.promote(deployment,
            new ZeroDowntimeUpdateCoordinator.PromotionEvidence(deployment, WORLD, TARGET, 13, true, true, true));
        assertEquals(Phase.PROMOTED, promoted.phase());

        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.complete(deployment, new ZeroDowntimeUpdateCoordinator.CompletionEvidence(
                deployment, WORLD, TARGET, 13, true, true, true, false)));
        WorldUpdateState completed = coordinator.complete(deployment,
            new ZeroDowntimeUpdateCoordinator.CompletionEvidence(deployment, WORLD, TARGET, 13,
                true, true, true, true));
        assertEquals(Phase.COMPLETED, completed.phase());
    }

    @Test void incompleteWorldJournalCannotFreezeOrReachPromotion() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 4, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID deployment = UUID.randomUUID();
        coordinator.begin(deployment, WORLD, TARGET);
        coordinator.beginCatchUp(deployment, 7, DIGEST,
            Set.of(Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT, Capability.STANDBY_NON_AUTHORITATIVE));
        coordinator.reportCatchUp(deployment, 7,
            Set.of(Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT, Capability.STANDBY_NON_AUTHORITATIVE));

        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class, () -> coordinator.freezeAtBarrier(deployment,
            new ZeroDowntimeUpdateCoordinator.FreezeEvidence(deployment, WORLD, SOURCE, 8,
                true, true, true, true)));
        assertEquals(Phase.CATCHING_UP, store.findUpdate(deployment).orElseThrow().phase());
        assertEquals(SOURCE, store.findAuthority(WORLD).orElseThrow().ownerInstance());
    }

    @Test void missingContinuityCapabilitiesCannotBecomeReadyEvenAfterTheBarrier() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 2, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID deployment = UUID.randomUUID();
        coordinator.begin(deployment, WORLD, TARGET);
        var preFreeze = preFreezeCapabilities();
        coordinator.beginCatchUp(deployment, 10, DIGEST, preFreeze);
        coordinator.reportCatchUp(deployment, 10, preFreeze);
        coordinator.freezeAtBarrier(deployment,
            new ZeroDowntimeUpdateCoordinator.FreezeEvidence(deployment, WORLD, SOURCE, 10,
                true, true, true, true));
        coordinator.reportCatchUp(deployment, 10, preFreeze);

        var withoutWorldJournal = new java.util.HashSet<>(fullCapabilities());
        withoutWorldJournal.remove(Capability.COMPLETE_WORLD_MUTATION_JOURNAL);
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.markReady(deployment, readiness(deployment, 10, withoutWorldJournal)));
        assertEquals(Phase.FROZEN, store.findUpdate(deployment).orElseThrow().phase());
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class, () -> coordinator.fenceAuthority(deployment));
        assertEquals(2, store.findAuthority(WORLD).orElseThrow().epoch());
    }

    @Test void preFenceRecoveryReturnsOnlyToUnchangedSourceAuthority() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 9, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID deployment = UUID.randomUUID();
        coordinator.begin(deployment, WORLD, TARGET);

        WorldUpdateState recovering = coordinator.recover(deployment);
        assertEquals(Phase.RECOVERING, recovering.phase());
        assertEquals(RecoveryMode.SOURCE_BEFORE_FENCE, recovering.recoveryMode());
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.finishRecoveryOnTarget(deployment,
                new ZeroDowntimeUpdateCoordinator.RecoveryEvidence(deployment, WORLD, TARGET, 10, true, true, true)));
        assertEquals(Phase.ABORTED, coordinator.finishRecoveryOnSource(deployment).phase());
        assertEquals(new WorldAuthority(WORLD, SOURCE, 9, null), store.findAuthority(WORLD).orElseThrow());
    }

    @Test void onlyOneDeploymentCanReserveAWorldAndSourceRecoveryReleasesTheReservation() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 9, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        coordinator.begin(first, WORLD, TARGET);
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.begin(second, WORLD, "paper-standby-c"));

        coordinator.recover(first);
        coordinator.finishRecoveryOnSource(first);
        assertEquals(Phase.PREPARING, coordinator.begin(second, WORLD, "paper-standby-c").phase());
    }

    @Test void concurrentDeploymentIdsCannotBothReserveTheSameWorld() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 9, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        var ready = new java.util.concurrent.CountDownLatch(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> attemptBegin(coordinator, UUID.randomUUID(), "target-a", ready, start));
            var second = pool.submit(() -> attemptBegin(coordinator, UUID.randomUUID(), "target-b", ready, start));
            assertTrue(ready.await(2, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            assertEquals(1, (first.get(2, java.util.concurrent.TimeUnit.SECONDS) ? 1 : 0)
                + (second.get(2, java.util.concurrent.TimeUnit.SECONDS) ? 1 : 0));
            assertEquals(1, store.activeDeployments.size());
        } finally { pool.shutdownNow(); }
    }

    @Test void postFenceRecoveryKeepsNewEpochAndCannotRollAuthorityBack() throws Exception {
        var store = readyStore();
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID deployment = store.onlyDeployment();
        coordinator.fenceAuthority(deployment);

        WorldUpdateState recovering = coordinator.recover(deployment);
        assertEquals(RecoveryMode.TARGET_AFTER_FENCE, recovering.recoveryMode());
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.finishRecoveryOnSource(deployment));
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class,
            () -> coordinator.finishRecoveryOnTarget(deployment,
                new ZeroDowntimeUpdateCoordinator.RecoveryEvidence(deployment, WORLD, TARGET, 13, true, false, true)));
        assertEquals(Phase.COMPLETED, coordinator.finishRecoveryOnTarget(deployment,
            new ZeroDowntimeUpdateCoordinator.RecoveryEvidence(deployment, WORLD, TARGET, 13, true, true, true)).phase());
        assertEquals(new WorldAuthority(WORLD, TARGET, 13, deployment), store.findAuthority(WORLD).orElseThrow());
    }

    @Test void staleFenceCannotPromoteOrReplaceAnAuthorityThatChanged() throws Exception {
        var store = readyStore();
        UUID deployment = store.onlyDeployment();
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        WorldUpdateState ready = store.findUpdate(deployment).orElseThrow();
        WorldUpdateState fenced = new WorldUpdateState(ready.deploymentId(), ready.worldId(), ready.sourceInstance(),
            ready.targetInstance(), ready.sourceEpoch(), Phase.FENCED, ready.revision() + 1,
            ready.checkpointSequence(), ready.checkpointSha256(), ready.replayedSequence(), ready.barrierSequence(),
            ready.capabilities(), null);
        store.forceAuthority(new WorldAuthority(WORLD, "unexpected-owner", 13, null));
        assertFalse(store.fenceAuthority(ready, fenced));
        assertThrows(ZeroDowntimeUpdateCoordinator.Conflict.class, () -> coordinator.fenceAuthority(deployment));
    }

    private static MemoryUpdateStore readyStore() throws Exception {
        var store = new MemoryUpdateStore();
        store.initializeAuthority(new WorldAuthority(WORLD, SOURCE, 12, null));
        var coordinator = new ZeroDowntimeUpdateCoordinator(store);
        UUID deployment = UUID.randomUUID();
        coordinator.begin(deployment, WORLD, TARGET);
        coordinator.beginCatchUp(deployment, 100, DIGEST, preFreezeCapabilities());
        coordinator.reportCatchUp(deployment, 108, preFreezeCapabilities());
        coordinator.freezeAtBarrier(deployment,
            new ZeroDowntimeUpdateCoordinator.FreezeEvidence(deployment, WORLD, SOURCE, 108, true, true, true, true));
        coordinator.reportCatchUp(deployment, 108, fullCapabilities());
        coordinator.markReady(deployment, readiness(deployment, 108, fullCapabilities()));
        return store;
    }

    private static ZeroDowntimeUpdateCoordinator.ReadinessEvidence readiness(UUID deployment, long applied,
                                                                               Set<Capability> capabilities) {
        return new ZeroDowntimeUpdateCoordinator.ReadinessEvidence(deployment, WORLD, TARGET,
            100, DIGEST, applied, capabilities);
    }

    private static Set<Capability> preFreezeCapabilities() {
        return Set.of(Capability.CRASH_CONSISTENT_WORLD_CHECKPOINT,
            Capability.COMPLETE_WORLD_MUTATION_JOURNAL,
            Capability.DETERMINISTIC_ORDERED_REPLAY,
            Capability.GLOBAL_AUTHORITY_FENCING,
            Capability.STANDBY_NON_AUTHORITATIVE);
    }

    private static Set<Capability> fullCapabilities() { return Set.copyOf(Set.of(Capability.values())); }

    private static boolean attemptBegin(ZeroDowntimeUpdateCoordinator coordinator, UUID deployment,
                                       String target, java.util.concurrent.CountDownLatch ready,
                                       java.util.concurrent.CountDownLatch start) throws Exception {
        ready.countDown();
        start.await();
        try {
            coordinator.begin(deployment, WORLD, target);
            return true;
        } catch (ZeroDowntimeUpdateCoordinator.Conflict conflict) {
            return false;
        }
    }

    private static final class MemoryUpdateStore implements WorldUpdateStore {
        private final Map<String, WorldAuthority> authorities = new HashMap<>();
        private final Map<UUID, WorldUpdateState> updates = new HashMap<>();
        private final Map<String, UUID> activeDeployments = new HashMap<>();

        @Override public synchronized Optional<WorldAuthority> findAuthority(String worldId) {
            return Optional.ofNullable(authorities.get(worldId));
        }

        @Override public synchronized boolean initializeAuthority(WorldAuthority initial) {
            return authorities.putIfAbsent(initial.worldId(), initial) == null;
        }

        @Override public synchronized Optional<WorldUpdateState> findUpdate(UUID deploymentId) {
            return Optional.ofNullable(updates.get(deploymentId));
        }

        @Override public synchronized boolean beginIfAuthority(WorldUpdateState initial) {
            WorldAuthority authority = authorities.get(initial.worldId());
            if (authority == null || !authority.ownerInstance().equals(initial.sourceInstance())
                    || authority.epoch() != initial.sourceEpoch() || updates.containsKey(initial.deploymentId())
                    || activeDeployments.containsKey(initial.worldId())) return false;
            updates.put(initial.deploymentId(), initial);
            activeDeployments.put(initial.worldId(), initial.deploymentId());
            return true;
        }

        @Override public synchronized boolean compareAndSet(long expectedRevision, WorldUpdateState next) {
            WorldUpdateState old = updates.get(next.deploymentId());
            if (old == null || old.revision() != expectedRevision || next.revision() != expectedRevision + 1) return false;
            if (next.phase() == Phase.ABORTED || next.phase() == Phase.COMPLETED) {
                if (!next.deploymentId().equals(activeDeployments.get(next.worldId()))) return false;
                activeDeployments.remove(next.worldId());
            }
            updates.put(next.deploymentId(), next);
            return true;
        }

        @Override public synchronized boolean fenceAuthority(WorldUpdateState expectedReady, WorldUpdateState fenced) {
            WorldUpdateState state = updates.get(expectedReady.deploymentId());
            WorldAuthority authority = authorities.get(expectedReady.worldId());
            if (!expectedReady.equals(state) || authority == null
                    || !authority.ownerInstance().equals(expectedReady.sourceInstance())
                    || authority.epoch() != expectedReady.sourceEpoch()
                    || fenced.phase() != Phase.FENCED || fenced.revision() != expectedReady.revision() + 1
                    || !fenced.hasRequiredPromotionCapabilities()) return false;
            updates.put(fenced.deploymentId(), fenced);
            authorities.put(fenced.worldId(), new WorldAuthority(fenced.worldId(), fenced.targetInstance(),
                fenced.sourceEpoch() + 1, fenced.deploymentId()));
            return true;
        }

        synchronized void forceAuthority(WorldAuthority authority) {
            authorities.put(authority.worldId(), authority);
        }

        UUID onlyDeployment() { return updates.keySet().iterator().next(); }
    }
}
