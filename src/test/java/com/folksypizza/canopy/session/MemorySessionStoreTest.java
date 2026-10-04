package com.folksypizza.canopy.session;

import org.junit.jupiter.api.Test;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import static org.junit.jupiter.api.Assertions.*;

class MemorySessionStoreTest extends SessionStoreContract {
    @Override SessionStore createStore() {
        return new SessionStore() {
            final ConcurrentHashMap<UUID, PlayerSession> rows = new ConcurrentHashMap<>();
            public Optional<PlayerSession> find(UUID id) { return Optional.ofNullable(rows.get(id)); }
            public boolean insert(PlayerSession s) { return rows.putIfAbsent(s.playerId(), s) == null; }
            public boolean compareAndSet(long expected, PlayerSession next) {
                PlayerSession old = rows.get(next.playerId());
                return old != null && old.revision() == expected && rows.replace(next.playerId(), old, next);
            }
        };
    }

    @Test void failedFinalSaveDoesNotPublishOfflineOrLoseSnapshot() throws SQLException {
        coordinator.checkpoint(active.authorityToken(), 1, new byte[]{4});
        coordinator.disconnect(active.gatewayToken());
        SessionCoordinator unavailable = new SessionCoordinator(new SessionStore() {
            public Optional<PlayerSession> find(UUID id) throws SQLException { return store.find(id); }
            public boolean insert(PlayerSession s) throws SQLException { throw new SQLException("test outage"); }
            public boolean compareAndSet(long expected, PlayerSession s) throws SQLException { throw new SQLException("test outage"); }
        });
        assertThrows(SQLException.class, () -> unavailable.finishLogout(active.authorityToken(), 2, new byte[]{5}));
        PlayerSession held = store.find(player).orElseThrow();
        assertEquals(PlayerSession.Phase.CLOSING, held.phase());
        assertArrayEquals(new byte[]{4}, held.snapshot());
        PlayerSession offline = coordinator.finishLogout(active.authorityToken(), 2, new byte[]{5});
        assertArrayEquals(new byte[]{5}, offline.snapshot());
    }
}
