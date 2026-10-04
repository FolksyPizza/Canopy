package com.folksypizza.canopy.session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mariadb.jdbc.MariaDbDataSource;
import javax.sql.DataSource;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/** Run only against an explicitly supplied disposable loopback database. */
@EnabledIfSystemProperty(named = "canopy.test.jdbc", matches = "jdbc:mariadb://127\\.0\\.0\\.1:[0-9]+/canopy_session_test")
class JdbcSessionStoreTest extends SessionStoreContract {
    DataSource dataSource;

    @Override SessionStore createStore() throws SQLException {
        MariaDbDataSource ds = new MariaDbDataSource();
        ds.setUrl(System.getProperty("canopy.test.jdbc"));
        ds.setUser("root");
        dataSource = ds;
        JdbcSessionStore result = new JdbcSessionStore(ds);
        result.initialize();
        return result;
    }

    @Test void newStoreInstanceReadsPersistedOwnershipAndSnapshot() throws SQLException {
        coordinator.checkpoint(active.authorityToken(), 1, new byte[]{9});
        PlayerSession recovered = new JdbcSessionStore(dataSource).find(player).orElseThrow();
        assertEquals(active.sessionId(), recovered.sessionId());
        assertEquals("shard-a", recovered.owner());
        assertArrayEquals(new byte[]{9}, recovered.snapshot());
    }

    @Test void corruptPayloadFailsInsteadOfReturningAnEmptyPlayer() throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement s = c.prepareStatement("UPDATE canopy_player_sessions SET state=? WHERE player_uuid=?")) {
            s.setBytes(1, new byte[]{1, 2, 3});
            s.setString(2, player.toString());
            s.executeUpdate();
        }
        assertThrows(SQLException.class, () -> store.find(player));
    }
}
