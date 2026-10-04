package com.folksypizza.canopy.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.*;

import static com.folksypizza.canopy.session.PlayerSession.Phase.*;
import static org.junit.jupiter.api.Assertions.*;

abstract class SessionStoreContract {
    @org.junit.jupiter.api.Test void deathFencesSnapshotsCapturedBeforeDeathAndPersistsThroughReconnect() throws Exception {
        PlayerSession dead = coordinator.death(active.authorityToken(), 1, new byte[]{0, 7});
        assertEquals(PlayerSession.Life.DEAD, dead.life());
        assertEquals(1, dead.lifeRevision());
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.checkpoint(active.authorityToken(), 2, new byte[]{9}));
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.prepareHandover(active.authorityToken(),
            java.util.UUID.randomUUID(), "shard-b", 2, new byte[]{9}));
        coordinator.disconnect(dead.gatewayToken());
        coordinator.finishLogout(dead.authorityToken(), 1, new byte[]{0, 8});
        PlayerSession reconnect = coordinator.open(player, java.util.UUID.randomUUID(), "gateway-b");
        assertEquals(PlayerSession.Life.DEAD, reconnect.life());
        assertEquals(dead.lifeRevision(), reconnect.lifeRevision());
        assertArrayEquals(new byte[]{0, 8}, reconnect.snapshot());
    }

    @org.junit.jupiter.api.Test void respawnFencesDeadSavesAndRejectsRepeatedRespawn() throws Exception {
        PlayerSession dead = coordinator.death(active.authorityToken(), 1, new byte[]{0});
        PlayerSession alive = coordinator.respawn(dead.authorityToken(), dead.lifeRevision(), 1, new byte[]{20});
        assertEquals(PlayerSession.Life.ALIVE, alive.life());
        assertEquals(2, alive.lifeRevision());
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.checkpoint(dead.authorityToken(), 2, new byte[]{0}));
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.respawn(dead.authorityToken(), dead.lifeRevision(), 2, new byte[]{20}));
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.finishLogout(active.authorityToken(), 3, new byte[]{99}));
    }

    @org.junit.jupiter.api.Test void deadOwnershipCanMoveBeforeTheOneRealRespawn() throws Exception {
        PlayerSession dead = coordinator.death(active.authorityToken(), 1, new byte[]{0});
        java.util.UUID operation = java.util.UUID.randomUUID();
        PlayerSession cut = coordinator.prepareHandover(dead.authorityToken(), operation, "shard-b", 1, new byte[]{0});
        PlayerSession moved = coordinator.commitHandover(cut.gatewayToken(), operation, cut.epoch());
        assertEquals(PlayerSession.Life.DEAD, moved.life());
        assertEquals(dead.lifeRevision(), moved.lifeRevision());
        assertEquals("shard-b", moved.owner());
        PlayerSession respawned = coordinator.respawn(moved.authorityToken(), moved.lifeRevision(), 1, new byte[]{20});
        assertEquals(2, respawned.lifeRevision());
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.respawn(dead.authorityToken(), dead.lifeRevision(), 2, new byte[]{20}));
    }
    @Test void promotionPersistsTheAppliedDestinationBeforeItsFirstCheckpoint() throws Exception {
        UUID transfer = UUID.randomUUID();
        coordinator.prepareHandover(active.authorityToken(), transfer, "shard-b", 1, new byte[]{1});
        PlayerSession destination = coordinator.commitHandover(active.gatewayToken(), transfer, active.epoch(), new byte[]{2});
        assertArrayEquals(new byte[]{2}, store.find(player).orElseThrow().snapshot());
        coordinator.disconnect(destination.gatewayToken());
        coordinator.finishFromCheckpoint(destination.gatewayToken());
        PlayerSession reconnect = coordinator.open(player, UUID.randomUUID(), "gateway-b");
        assertArrayEquals(new byte[]{2}, reconnect.snapshot());
    }

    SessionStore store;
    SessionCoordinator coordinator;
    UUID player;
    PlayerSession active;

    abstract SessionStore createStore() throws SQLException;

    @BeforeEach void setUp() throws SQLException {
        store = createStore();
        coordinator = new SessionCoordinator(store);
        player = UUID.randomUUID();
        PlayerSession open = coordinator.open(player, UUID.randomUUID(), "gateway-a");
        active = coordinator.attach(open.gatewayToken(), "shard-a");
    }

    @Test void backendHandoverKeepsGatewaySessionOnline() throws SQLException {
        UUID transfer = UUID.randomUUID();
        PlayerSession prepared = coordinator.prepareHandover(active.authorityToken(), transfer, "shard-b", 1, new byte[]{7});
        assertTrue(prepared.online());
        assertEquals(HANDOFF, prepared.phase());
        PlayerSession committed = coordinator.commitHandover(active.gatewayToken(), transfer, active.epoch());
        assertTrue(committed.online());
        assertEquals(active.sessionId(), committed.sessionId());
        assertEquals("shard-b", committed.owner());
        assertArrayEquals(new byte[]{7}, committed.snapshot());
        assertTrue(committed.epoch() > active.epoch());
    }

    @Test void staleSourceCannotOverwriteDestination() throws SQLException {
        UUID transfer = UUID.randomUUID();
        coordinator.prepareHandover(active.authorityToken(), transfer, "shard-b", 1, new byte[]{1});
        PlayerSession destination = coordinator.commitHandover(active.gatewayToken(), transfer, active.epoch());
        coordinator.checkpoint(destination.authorityToken(), 1, new byte[]{2});
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.checkpoint(active.authorityToken(), 99, new byte[]{3}));
        assertArrayEquals(new byte[]{2}, store.find(player).orElseThrow().snapshot());
    }

    @Test void frozenSourceCannotCheckpoint() throws SQLException {
        coordinator.prepareHandover(active.authorityToken(), UUID.randomUUID(), "shard-b", 1, new byte[]{1});
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.checkpoint(active.authorityToken(), 2, new byte[]{2}));
    }

    @Test void cancelledHandoverRequiresFreshAuthority() throws SQLException {
        UUID transfer = UUID.randomUUID();
        coordinator.prepareHandover(active.authorityToken(), transfer, "shard-b", 1, new byte[]{1});
        PlayerSession resumed = coordinator.cancelHandover(active.gatewayToken(), transfer, active.epoch());
        assertEquals("shard-a", resumed.owner());
        assertTrue(resumed.online());
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.checkpoint(active.authorityToken(), 2, new byte[]{2}));
        coordinator.checkpoint(resumed.authorityToken(), 1, new byte[]{3});
    }

    @Test void finalLogoutSnapshotIsLoadedOnAnotherShard() throws SQLException {
        coordinator.checkpoint(active.authorityToken(), 1, new byte[]{1});
        PlayerSession closing = coordinator.disconnect(active.gatewayToken());
        assertFalse(closing.online());
        assertEquals(CLOSING, closing.phase());
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.open(player, UUID.randomUUID(), "gateway-b"));
        PlayerSession offline = coordinator.finishLogout(active.authorityToken(), 2, new byte[]{2});
        assertEquals(OFFLINE, offline.phase());
        assertNull(offline.owner());
        SessionCoordinator otherGateway = new SessionCoordinator(store);
        PlayerSession reconnect = otherGateway.open(player, UUID.randomUUID(), "gateway-b");
        PlayerSession attached = otherGateway.attach(reconnect.gatewayToken(), "shard-c");
        assertArrayEquals(new byte[]{2}, attached.snapshot());
    }

    @Test void delayedDisconnectAndSaveCannotCloseReconnectedPlayer() throws SQLException {
        coordinator.disconnect(active.gatewayToken());
        coordinator.finishLogout(active.authorityToken(), 1, new byte[]{4});
        PlayerSession reconnect = coordinator.open(player, UUID.randomUUID(), "gateway-a");
        coordinator.attach(reconnect.gatewayToken(), "shard-a");
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.disconnect(active.gatewayToken()));
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.finishLogout(active.authorityToken(), 2, new byte[]{5}));
        assertTrue(store.find(player).orElseThrow().online());
    }

    @Test void disconnectDuringCutUsesCutSnapshotAndCannotPromote() throws SQLException {
        UUID transfer = UUID.randomUUID();
        coordinator.prepareHandover(active.authorityToken(), transfer, "shard-b", 1, new byte[]{8});
        coordinator.disconnect(active.gatewayToken());
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.commitHandover(active.gatewayToken(), transfer, active.epoch()));
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.finishLogout(active.authorityToken(), 2, new byte[]{9}));
        PlayerSession offline = coordinator.finishFromCheckpoint(active.gatewayToken());
        assertArrayEquals(new byte[]{8}, offline.snapshot());
    }

    @Test void crashFinalizationKeepsLastDurableCheckpoint() throws SQLException {
        coordinator.checkpoint(active.authorityToken(), 1, new byte[]{6});
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.finishFromCheckpoint(active.gatewayToken()));
        coordinator.disconnect(active.gatewayToken());
        PlayerSession offline = coordinator.finishFromCheckpoint(active.gatewayToken());
        assertFalse(offline.online());
        assertArrayEquals(new byte[]{6}, offline.snapshot());
    }

    @Test void staleSnapshotSequenceIsRejected() throws SQLException {
        coordinator.checkpoint(active.authorityToken(), 5, new byte[]{5});
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.checkpoint(active.authorityToken(), 4, new byte[]{4}));
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.checkpoint(active.authorityToken(), 5, new byte[]{4}));
    }

    @Test void emptyAndOversizedSnapshotsDoNotAdvanceRevision() throws SQLException {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.checkpoint(active.authorityToken(), 1, new byte[0]));
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.checkpoint(active.authorityToken(), 1, new byte[PlayerSession.MAX_SNAPSHOT_BYTES + 1]));
        assertEquals(active.revision(), store.find(player).orElseThrow().revision());
    }

    @Test void callerCannotMutateStoredSnapshot() throws SQLException {
        byte[] blob = {3};
        PlayerSession saved = coordinator.checkpoint(active.authorityToken(), 1, blob);
        blob[0] = 9;
        saved.snapshot()[0] = 7;
        assertArrayEquals(new byte[]{3}, store.find(player).orElseThrow().snapshot());
    }

    @Test void onlyOneConcurrentOwnerUpdateWins() throws Exception {
        PlayerSession first = new PlayerSession(player, active.sessionId(), "gateway-a", ACTIVE,
            "shard-a", null, null, active.epoch(), active.revision() + 1, 1, new byte[]{1});
        PlayerSession second = new PlayerSession(player, active.sessionId(), "gateway-a", ACTIVE,
            "shard-a", null, null, active.epoch(), active.revision() + 1, 2, new byte[]{2});
        try (ExecutorService workers = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            Future<Boolean> a = workers.submit(() -> { start.await(); return store.compareAndSet(active.revision(), first); });
            Future<Boolean> b = workers.submit(() -> { start.await(); return store.compareAndSet(active.revision(), second); });
            start.countDown();
            assertNotEquals(a.get(5, TimeUnit.SECONDS), b.get(5, TimeUnit.SECONDS));
        }
        assertEquals(active.revision() + 1, store.find(player).orElseThrow().revision());
    }

    @Test void onlyOneConcurrentSessionInsertWins() throws Exception {
        UUID newPlayer = UUID.randomUUID();
        PlayerSession a = new PlayerSession(newPlayer, UUID.randomUUID(), "gateway-a", CONNECTED,
            null, null, null, 1, 1, 0, new byte[0]);
        PlayerSession b = new PlayerSession(newPlayer, UUID.randomUUID(), "gateway-b", CONNECTED,
            null, null, null, 1, 1, 0, new byte[0]);
        try (ExecutorService workers = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            Future<Boolean> first = workers.submit(() -> { start.await(); return store.insert(a); });
            Future<Boolean> second = workers.submit(() -> { start.await(); return store.insert(b); });
            start.countDown();
            assertNotEquals(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
        }
    }

    @Test void gatewayIdentityAndTransferIdentityAreChecked() throws SQLException {
        PlayerSession.GatewayToken impostor = new PlayerSession.GatewayToken(player, active.sessionId(), "gateway-b");
        assertThrows(SessionCoordinator.Conflict.class, () -> coordinator.disconnect(impostor));
        coordinator.prepareHandover(active.authorityToken(), UUID.randomUUID(), "shard-b", 1, new byte[]{1});
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.commitHandover(active.gatewayToken(), UUID.randomUUID(), active.epoch()));
    }

    @Test void closedSessionIdCannotBeReused() throws SQLException {
        coordinator.disconnect(active.gatewayToken());
        coordinator.finishFromCheckpoint(active.gatewayToken());
        assertThrows(SessionCoordinator.Conflict.class,
            () -> coordinator.open(player, active.sessionId(), "gateway-a"));
    }

    @Test void reconnectWithNoBackendRetainsSavedState() throws SQLException {
        coordinator.disconnect(active.gatewayToken());
        coordinator.finishLogout(active.authorityToken(), 1, new byte[]{1});
        PlayerSession connected = coordinator.open(player, UUID.randomUUID(), "gateway-b");
        coordinator.disconnect(connected.gatewayToken());
        PlayerSession offline = coordinator.finishFromCheckpoint(connected.gatewayToken());
        assertArrayEquals(new byte[]{1}, offline.snapshot());
    }
}
