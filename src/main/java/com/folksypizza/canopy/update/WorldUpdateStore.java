package com.folksypizza.canopy.update;

import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable compare-and-set contract for update records and world authority.
 * Implementations must serialize {@link #beginIfAuthority(WorldUpdateState)} per world so only one update can be
 * active at a time, remove that reservation when an update becomes terminal, and make
 * {@link #fenceAuthority(WorldUpdateState, WorldUpdateState)} atomic across authority and update rows.
 */
public interface WorldUpdateStore {
    Optional<WorldAuthority> findAuthority(String worldId) throws SQLException;

    /** Installs an initial authority row only when no row exists. */
    boolean initializeAuthority(WorldAuthority initial) throws SQLException;

    Optional<WorldUpdateState> findUpdate(UUID deploymentId) throws SQLException;

    /** Inserts a new update only if its source owner and epoch still hold authority for the world. */
    boolean beginIfAuthority(WorldUpdateState initial) throws SQLException;

    /** Replaces an update row only when its current revision equals {@code expectedRevision}. */
    boolean compareAndSet(long expectedRevision, WorldUpdateState next) throws SQLException;

    /**
     * Atomically advances world authority from the recorded source epoch to the target and changes READY to FENCED.
     * A false result means either row changed; callers must re-read instead of retrying stale state.
     */
    boolean fenceAuthority(WorldUpdateState expectedReady, WorldUpdateState fenced) throws SQLException;
}
