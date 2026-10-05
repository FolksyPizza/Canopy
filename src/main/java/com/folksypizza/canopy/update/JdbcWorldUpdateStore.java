package com.folksypizza.canopy.update;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.UUID;

/** MariaDB/MySQL-backed durable world-update and authority store. */
public final class JdbcWorldUpdateStore implements WorldUpdateStore {
    private final DataSource dataSource;

    public JdbcWorldUpdateStore(DataSource dataSource) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource);
    }

    public void initialize() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS canopy_world_authority (
                    world_id VARCHAR(128) PRIMARY KEY,
                    owner_instance VARCHAR(128) NOT NULL,
                    epoch BIGINT NOT NULL,
                    deployment_id CHAR(36) NULL
                ) ENGINE=InnoDB
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS canopy_zero_downtime_updates (
                    deployment_id CHAR(36) PRIMARY KEY,
                    world_id VARCHAR(128) NOT NULL,
                    source_instance VARCHAR(128) NOT NULL,
                    target_instance VARCHAR(128) NOT NULL,
                    source_epoch BIGINT NOT NULL,
                    phase VARCHAR(32) NOT NULL,
                    revision BIGINT NOT NULL,
                    checkpoint_sequence BIGINT NOT NULL,
                    checkpoint_sha256 CHAR(64) NULL,
                    replayed_sequence BIGINT NOT NULL,
                    barrier_sequence BIGINT NOT NULL,
                    capability_bits BIGINT NOT NULL,
                    recovery_mode VARCHAR(32) NULL,
                INDEX canopy_update_world_phase (world_id, phase)
                ) ENGINE=InnoDB
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS canopy_active_world_updates (
                    world_id VARCHAR(128) PRIMARY KEY,
                    deployment_id CHAR(36) NOT NULL UNIQUE
                ) ENGINE=InnoDB
                """);
        }
    }

    @Override public Optional<WorldAuthority> findAuthority(String worldId) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_instance, epoch, deployment_id FROM canopy_world_authority WHERE world_id=?")) {
            statement.setString(1, worldId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                return Optional.of(new WorldAuthority(worldId, rows.getString(1), rows.getLong(2), uuid(rows.getString(3))));
            }
        }
    }

    @Override public boolean initializeAuthority(WorldAuthority initial) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO canopy_world_authority (world_id, owner_instance, epoch, deployment_id) VALUES (?, ?, ?, ?)")) {
            statement.setString(1, initial.worldId());
            statement.setString(2, initial.ownerInstance());
            statement.setLong(3, initial.epoch());
            statement.setString(4, initial.deploymentId() == null ? null : initial.deploymentId().toString());
            return statement.executeUpdate() == 1;
        } catch (SQLException duplicate) {
            if (duplicate.getErrorCode() == 1062) return false;
            throw duplicate;
        }
    }

    @Override public Optional<WorldUpdateState> findUpdate(UUID deploymentId) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return selectUpdate(connection, deploymentId, false);
        }
    }

    @Override public boolean beginIfAuthority(WorldUpdateState initial) throws SQLException {
        requirePhase(initial, WorldUpdateState.Phase.PREPARING);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WorldAuthority authority = selectAuthority(connection, initial.worldId(), true).orElse(null);
                if (!matchesSource(authority, initial)) {
                    connection.rollback();
                    return false;
                }
                try (PreparedStatement active = connection.prepareStatement(
                        "SELECT deployment_id FROM canopy_active_world_updates WHERE world_id=? FOR UPDATE")) {
                    active.setString(1, initial.worldId());
                    try (ResultSet rows = active.executeQuery()) {
                        if (rows.next()) {
                            connection.rollback();
                            return false;
                        }
                    }
                }
                try (PreparedStatement reserve = connection.prepareStatement(
                        "INSERT INTO canopy_active_world_updates (world_id, deployment_id) VALUES (?, ?)")) {
                    reserve.setString(1, initial.worldId());
                    reserve.setString(2, initial.deploymentId().toString());
                    reserve.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO canopy_zero_downtime_updates
                        (deployment_id, world_id, source_instance, target_instance, source_epoch, phase, revision,
                         checkpoint_sequence, checkpoint_sha256, replayed_sequence, barrier_sequence,
                         capability_bits, recovery_mode)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)) {
                    bindUpdate(statement, initial);
                    statement.executeUpdate();
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                if (failure instanceof SQLException sqlFailure && sqlFailure.getErrorCode() == 1062) return false;
                throw failure;
            }
        }
    }

    @Override public boolean compareAndSet(long expectedRevision, WorldUpdateState next) throws SQLException {
        if (next.revision() != expectedRevision + 1) throw new IllegalArgumentException("Revision must advance by one");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WorldUpdateState stored = selectUpdate(connection, next.deploymentId(), true).orElse(null);
                if (stored == null || stored.revision() != expectedRevision
                        || !stored.worldId().equals(next.worldId())) {
                    connection.rollback();
                    return false;
                }
                try (PreparedStatement statement = connection.prepareStatement("""
                        UPDATE canopy_zero_downtime_updates SET phase=?, revision=?, checkpoint_sequence=?, checkpoint_sha256=?,
                            replayed_sequence=?, barrier_sequence=?, capability_bits=?, recovery_mode=?
                        WHERE deployment_id=? AND revision=?
                        """)) {
                    bindChangedState(statement, next);
                    statement.setString(9, next.deploymentId().toString());
                    statement.setLong(10, expectedRevision);
                    if (statement.executeUpdate() != 1) {
                        connection.rollback();
                        return false;
                    }
                }
                if (terminal(next.phase())) {
                    try (PreparedStatement release = connection.prepareStatement(
                            "DELETE FROM canopy_active_world_updates WHERE world_id=? AND deployment_id=?")) {
                        release.setString(1, next.worldId());
                        release.setString(2, next.deploymentId().toString());
                        if (release.executeUpdate() != 1) {
                            connection.rollback();
                            return false;
                        }
                    }
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        }
    }

    @Override public boolean fenceAuthority(WorldUpdateState expectedReady, WorldUpdateState fenced) throws SQLException {
        requirePhase(expectedReady, WorldUpdateState.Phase.READY);
        requirePhase(fenced, WorldUpdateState.Phase.FENCED);
        if (!samePlan(expectedReady, fenced) || fenced.revision() != expectedReady.revision() + 1
                || fenced.capabilities().isEmpty() || !fenced.hasRequiredPromotionCapabilities()) {
            throw new IllegalArgumentException("Invalid fenced transition");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WorldUpdateState stored = selectUpdate(connection, expectedReady.deploymentId(), true).orElse(null);
                WorldAuthority authority = selectAuthority(connection, expectedReady.worldId(), true).orElse(null);
                if (!expectedReady.equals(stored) || !matchesSource(authority, expectedReady)) {
                    connection.rollback();
                    return false;
                }
                try (PreparedStatement authorityUpdate = connection.prepareStatement("""
                        UPDATE canopy_world_authority SET owner_instance=?, epoch=?, deployment_id=?
                        WHERE world_id=? AND owner_instance=? AND epoch=?
                        """)) {
                    authorityUpdate.setString(1, fenced.targetInstance());
                    authorityUpdate.setLong(2, fenced.sourceEpoch() + 1);
                    authorityUpdate.setString(3, fenced.deploymentId().toString());
                    authorityUpdate.setString(4, fenced.worldId());
                    authorityUpdate.setString(5, fenced.sourceInstance());
                    authorityUpdate.setLong(6, fenced.sourceEpoch());
                    if (authorityUpdate.executeUpdate() != 1) {
                        connection.rollback();
                        return false;
                    }
                }
                try (PreparedStatement stateUpdate = connection.prepareStatement("""
                        UPDATE canopy_zero_downtime_updates SET phase=?, revision=?, checkpoint_sequence=?, checkpoint_sha256=?,
                            replayed_sequence=?, barrier_sequence=?, capability_bits=?, recovery_mode=?
                        WHERE deployment_id=? AND revision=?
                        """)) {
                    bindChangedState(stateUpdate, fenced);
                    stateUpdate.setString(9, fenced.deploymentId().toString());
                    stateUpdate.setLong(10, expectedReady.revision());
                    if (stateUpdate.executeUpdate() != 1) {
                        connection.rollback();
                        return false;
                    }
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        }
    }

    private static Optional<WorldUpdateState> selectUpdate(Connection connection, UUID deploymentId, boolean lock)
            throws SQLException {
        String sql = "SELECT world_id, source_instance, target_instance, source_epoch, phase, revision, "
            + "checkpoint_sequence, checkpoint_sha256, replayed_sequence, barrier_sequence, capability_bits, recovery_mode "
            + "FROM canopy_zero_downtime_updates WHERE deployment_id=?" + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, deploymentId.toString());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                return Optional.of(new WorldUpdateState(deploymentId, rows.getString(1), rows.getString(2), rows.getString(3),
                    rows.getLong(4), WorldUpdateState.Phase.valueOf(rows.getString(5)), rows.getLong(6), rows.getLong(7),
                    rows.getString(8), rows.getLong(9), rows.getLong(10),
                    WorldUpdateState.capabilitiesFromBits(rows.getLong(11)), recoveryMode(rows.getString(12))));
            }
        }
    }

    private static Optional<WorldAuthority> selectAuthority(Connection connection, String worldId, boolean lock)
            throws SQLException {
        String sql = "SELECT owner_instance, epoch, deployment_id FROM canopy_world_authority WHERE world_id=?"
            + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, worldId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                return Optional.of(new WorldAuthority(worldId, rows.getString(1), rows.getLong(2), uuid(rows.getString(3))));
            }
        }
    }

    private static void bindUpdate(PreparedStatement statement, WorldUpdateState state) throws SQLException {
        statement.setString(1, state.deploymentId().toString());
        statement.setString(2, state.worldId());
        statement.setString(3, state.sourceInstance());
        statement.setString(4, state.targetInstance());
        statement.setLong(5, state.sourceEpoch());
        statement.setString(6, state.phase().name());
        statement.setLong(7, state.revision());
        statement.setLong(8, state.checkpointSequence());
        statement.setString(9, state.checkpointSha256());
        statement.setLong(10, state.replayedSequence());
        statement.setLong(11, state.barrierSequence());
        statement.setLong(12, WorldUpdateState.capabilityBits(state.capabilities()));
        statement.setString(13, state.recoveryMode() == null ? null : state.recoveryMode().name());
    }

    private static void bindChangedState(PreparedStatement statement, WorldUpdateState state) throws SQLException {
        statement.setString(1, state.phase().name());
        statement.setLong(2, state.revision());
        statement.setLong(3, state.checkpointSequence());
        statement.setString(4, state.checkpointSha256());
        statement.setLong(5, state.replayedSequence());
        statement.setLong(6, state.barrierSequence());
        statement.setLong(7, WorldUpdateState.capabilityBits(state.capabilities()));
        statement.setString(8, state.recoveryMode() == null ? null : state.recoveryMode().name());
    }

    private static boolean matchesSource(WorldAuthority authority, WorldUpdateState state) {
        return authority != null && authority.worldId().equals(state.worldId())
            && authority.ownerInstance().equals(state.sourceInstance()) && authority.epoch() == state.sourceEpoch();
    }

    private static boolean samePlan(WorldUpdateState left, WorldUpdateState right) {
        return left.deploymentId().equals(right.deploymentId()) && left.worldId().equals(right.worldId())
            && left.sourceInstance().equals(right.sourceInstance()) && left.targetInstance().equals(right.targetInstance())
            && left.sourceEpoch() == right.sourceEpoch() && left.checkpointSequence() == right.checkpointSequence()
            && java.util.Objects.equals(left.checkpointSha256(), right.checkpointSha256())
            && left.replayedSequence() == right.replayedSequence() && left.barrierSequence() == right.barrierSequence()
            && left.capabilities().equals(right.capabilities());
    }

    private static boolean terminal(WorldUpdateState.Phase phase) {
        return phase == WorldUpdateState.Phase.ABORTED || phase == WorldUpdateState.Phase.COMPLETED;
    }

    private static void requirePhase(WorldUpdateState state, WorldUpdateState.Phase phase) {
        if (state.phase() != phase) throw new IllegalArgumentException("Expected " + phase + " update state");
    }

    private static UUID uuid(String value) { return value == null ? null : UUID.fromString(value); }
    private static WorldUpdateState.RecoveryMode recoveryMode(String value) {
        return value == null ? null : WorldUpdateState.RecoveryMode.valueOf(value);
    }

    private static void rollback(Connection connection, Throwable failure) {
        try { connection.rollback(); } catch (SQLException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
    }
}
