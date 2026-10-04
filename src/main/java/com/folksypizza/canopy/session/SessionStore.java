package com.folksypizza.canopy.session;

import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

/** Implementations must make insertion and revision comparison atomic across callers. */
public interface SessionStore {
    Optional<PlayerSession> find(UUID playerId) throws SQLException;
    boolean insert(PlayerSession initial) throws SQLException;
    boolean compareAndSet(long expectedRevision, PlayerSession next) throws SQLException;
}
