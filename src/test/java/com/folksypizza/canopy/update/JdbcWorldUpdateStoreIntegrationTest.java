package com.folksypizza.canopy.update;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mariadb.jdbc.MariaDbDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.folksypizza.canopy.update.WorldUpdateState.Capability;
import static com.folksypizza.canopy.update.WorldUpdateState.Phase;
import static com.folksypizza.canopy.update.WorldUpdateState.RecoveryMode;
import static org.junit.jupiter.api.Assertions.*;

/** Integration coverage for the MariaDB schema and transactional world-update operations. */
@EnabledIfSystemProperty(named = "canopy.test.world-update.jdbc",
    matches = "jdbc:mariadb://127\\.0\\.0\\.1:[0-9]+/canopy_world_update_test")
class JdbcWorldUpdateStoreIntegrationTest {
    private static final String SOURCE = "source-instance";
    private static final String DIGEST = "a".repeat(64);
    private final MariaDbDataSource dataSource;
    private final JdbcWorldUpdateStore store;

    JdbcWorldUpdateStoreIntegrationTest() throws SQLException {
        dataSource = dataSource();
        store = new JdbcWorldUpdateStore(dataSource);
    }

    @Test void initializesSchemaAndReleasesReservationAfterSourceRecovery() throws Exception {
        store.initialize();
        store.initialize();

        String world = "world-" + UUID.randomUUID();
        assertTrue(store.initializeAuthority(new WorldAuthority(world, SOURCE, 1, null)));
        assertFalse(store.initializeAuthority(new WorldAuthority(world, "other-source", 8, null)));

        UUID firstId = UUID.randomUUID();
        WorldUpdateState preparing = preparing(firstId, world, "target-a");
        assertTrue(store.beginIfAuthority(preparing));
        assertFalse(store.beginIfAuthority(preparing(UUID.randomUUID(), world, "target-b")));
        assertEquals(preparing, store.findUpdate(firstId).orElseThrow());

        WorldUpdateState recovering = copy(preparing, Phase.RECOVERING, 2, null,
            -1, -1, Set.of(), RecoveryMode.SOURCE_BEFORE_FENCE);
        assertTrue(store.compareAndSet(preparing.revision(), recovering));
        WorldUpdateState aborted = copy(recovering, Phase.ABORTED, 3, null,
            -1, -1, Set.of(), null);
        assertTrue(store.compareAndSet(recovering.revision(), aborted));

        UUID secondId = UUID.randomUUID();
        assertTrue(store.beginIfAuthority(preparing(secondId, world, "target-b")));
        assertEquals(SOURCE, store.findAuthority(world).orElseThrow().ownerInstance());
        releaseReservationThroughRecovery(secondId);
    }

    @Test void concurrentStartsReserveOneWorldAndFenceRollsBackAtomicallyOnSqlFailure() throws Exception {
        store.initialize();
        String world = "world-" + UUID.randomUUID();
        WorldAuthority initialAuthority = new WorldAuthority(world, SOURCE, 1, null);
        assertTrue(store.initializeAuthority(initialAuthority));

        UUID firstId = UUID.randomUUID();
        UUID secondId = UUID.randomUUID();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> attemptBegin(preparing(firstId, world, "target-a"), ready, start));
            var second = workers.submit(() -> attemptBegin(preparing(secondId, world, "target-b"), ready, start));
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            assertNotEquals(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        } finally {
            start.countDown();
        }

        UUID activeId = store.findUpdate(firstId).isPresent() ? firstId : secondId;
        WorldUpdateState preparing = store.findUpdate(activeId).orElseThrow();
        WorldUpdateState readyState = ready(preparing);
        assertTrue(store.compareAndSet(preparing.revision(), readyState));
        WorldUpdateState fenced = copy(readyState, Phase.FENCED, readyState.revision() + 1,
            DIGEST, 50, 50, readyState.capabilities(), null);

        String trigger = "fail_fence_" + UUID.randomUUID().toString().replace("-", "");
        try {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("CREATE TRIGGER `" + trigger + "` BEFORE UPDATE ON canopy_zero_downtime_updates "
                    + "FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'intentional integration-test failure'");
            }

            assertThrows(SQLException.class, () -> store.fenceAuthority(readyState, fenced));
            assertEquals(initialAuthority, store.findAuthority(world).orElseThrow());
            assertEquals(readyState, store.findUpdate(activeId).orElseThrow());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TRIGGER IF EXISTS `" + trigger + "`");
            }
        }

        assertTrue(store.fenceAuthority(readyState, fenced));
        assertEquals(new WorldAuthority(world, readyState.targetInstance(), initialAuthority.epoch() + 1, activeId),
            store.findAuthority(world).orElseThrow());
        assertEquals(fenced, store.findUpdate(activeId).orElseThrow());
        releaseReservationThroughRecovery(activeId);
    }

    private boolean attemptBegin(WorldUpdateState state, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out waiting to start");
        return store.beginIfAuthority(state);
    }

    private void releaseReservationThroughRecovery(UUID deploymentId) throws SQLException {
        WorldUpdateState current = store.findUpdate(deploymentId).orElseThrow();
        boolean pastFence = current.phase() == Phase.FENCED;
        WorldUpdateState recovering = copy(current, Phase.RECOVERING, current.revision() + 1,
            current.checkpointSha256(), current.replayedSequence(), current.barrierSequence(), current.capabilities(),
            pastFence ? RecoveryMode.TARGET_AFTER_FENCE : RecoveryMode.SOURCE_BEFORE_FENCE);
        assertTrue(store.compareAndSet(current.revision(), recovering));
        Phase terminal = pastFence ? Phase.COMPLETED : Phase.ABORTED;
        WorldUpdateState done = copy(recovering, terminal, recovering.revision() + 1,
            recovering.checkpointSha256(), recovering.replayedSequence(), recovering.barrierSequence(),
            recovering.capabilities(), null);
        assertTrue(store.compareAndSet(recovering.revision(), done));
    }

    private static WorldUpdateState preparing(UUID deployment, String world, String target) {
        return new WorldUpdateState(deployment, world, SOURCE, target, 1, Phase.PREPARING, 1,
            0, null, -1, -1, Set.of(), null);
    }

    private static WorldUpdateState ready(WorldUpdateState current) {
        return copy(current, Phase.READY, current.revision() + 1, DIGEST, 50, 50,
            Set.copyOf(Set.of(Capability.values())), null);
    }

    private static WorldUpdateState copy(WorldUpdateState current, Phase phase, long revision,
                                         String digest, long replayed, long barrier,
                                         Set<Capability> capabilities, RecoveryMode recoveryMode) {
        long checkpointSequence = digest == null ? current.checkpointSequence() : 50;
        return new WorldUpdateState(current.deploymentId(), current.worldId(), current.sourceInstance(),
            current.targetInstance(), current.sourceEpoch(), phase, revision, checkpointSequence, digest,
            replayed, barrier, capabilities, recoveryMode);
    }

    private static MariaDbDataSource dataSource() throws SQLException {
        MariaDbDataSource dataSource = new MariaDbDataSource();
        dataSource.setUrl(System.getProperty("canopy.test.world-update.jdbc"));
        dataSource.setUser("root");
        return dataSource;
    }
}
