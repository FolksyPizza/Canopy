package com.folksypizza.canopy.session;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** MariaDB/MySQL store: one atomic row contains both ownership and the latest committed snapshot. */
public final class JdbcSessionStore implements SessionStore {
    private final DataSource dataSource;

    /** The caller owns the connection pool and must configure connection/query timeouts. */
    public JdbcSessionStore(DataSource dataSource) { this.dataSource = Objects.requireNonNull(dataSource); }

    public void initialize() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS canopy_player_sessions (
                    player_uuid CHAR(36) PRIMARY KEY,
                    revision BIGINT NOT NULL,
                    state MEDIUMBLOB NOT NULL
                ) ENGINE=InnoDB
                """);
        }
    }

    @Override public Optional<PlayerSession> find(UUID player) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement s = c.prepareStatement("SELECT revision, state FROM canopy_player_sessions WHERE player_uuid=?")) {
            s.setString(1, player.toString());
            try (ResultSet r = s.executeQuery()) {
                if (!r.next()) return Optional.empty();
                PlayerSession state = SessionCodec.decode(r.getBytes(2));
                if (!state.playerId().equals(player) || state.revision() != r.getLong(1)) {
                    throw new SQLException("Session row metadata does not match payload");
                }
                return Optional.of(state);
            }
        }
    }

    @Override public boolean insert(PlayerSession initial) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement s = c.prepareStatement("INSERT INTO canopy_player_sessions (player_uuid, revision, state) VALUES (?, ?, ?)")) {
            requireAutoCommit(c);
            s.setString(1, initial.playerId().toString());
            s.setLong(2, initial.revision());
            s.setBytes(3, SessionCodec.encode(initial));
            return s.executeUpdate() == 1;
        } catch (SQLException e) {
            // Only a duplicate player row is a concurrency conflict. Other integrity/storage failures propagate.
            if (e.getErrorCode() == 1062) return false;
            throw e;
        }
    }

    @Override public boolean compareAndSet(long expected, PlayerSession next) throws SQLException {
        if (next.revision() != expected + 1) throw new IllegalArgumentException("Revision must advance by one");
        try (Connection c = dataSource.getConnection();
             PreparedStatement s = c.prepareStatement("UPDATE canopy_player_sessions SET revision=?, state=? WHERE player_uuid=? AND revision=?")) {
            requireAutoCommit(c);
            s.setLong(1, next.revision());
            s.setBytes(2, SessionCodec.encode(next));
            s.setString(3, next.playerId().toString());
            s.setLong(4, expected);
            return s.executeUpdate() == 1;
        }
    }

    private static void requireAutoCommit(Connection c) throws SQLException {
        if (!c.getAutoCommit()) throw new SQLException("Session store requires autocommit connections");
    }

}
