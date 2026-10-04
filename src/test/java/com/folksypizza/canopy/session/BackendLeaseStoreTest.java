package com.folksypizza.canopy.session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mariadb.jdbc.MariaDbDataSource;
import java.sql.SQLException;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "canopy.test.jdbc", matches = "jdbc:mariadb://127\\.0\\.0\\.1:[0-9]+/canopy_session_test")
class BackendLeaseStoreTest {
    @Test void liveProcessCannotBeReplacedAndOldShutdownCannotRemoveTheNewLease() throws Exception {
        var ds = new MariaDbDataSource(); ds.setUrl(System.getProperty("canopy.test.jdbc")); ds.setUser("root");
        var leases = new BackendLeaseStore(ds); leases.initialize();
        String shard = "test-" + UUID.randomUUID(), first = UUID.randomUUID().toString(), next = UUID.randomUUID().toString();
        try {
            leases.claim(shard, first);
            assertTrue(leases.alive(shard));
            assertThrows(SQLException.class, () -> leases.claim(shard, next));
            leases.release(shard, first);
            assertFalse(leases.alive(shard));
            leases.claim(shard, next);
            leases.release(shard, first);
            assertTrue(leases.alive(shard));
            assertThrows(SQLException.class, () -> leases.heartbeat(shard, first));
        } finally { leases.release(shard, next); leases.release(shard, first); }
    }
}
