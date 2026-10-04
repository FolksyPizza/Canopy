package com.folksypizza.canopy.session;

import javax.sql.DataSource;
import java.sql.*;

/** Database-clock liveness for checkpoint-only logout recovery after a simulation process stops. */
public final class BackendLeaseStore {
    private final DataSource dataSource;
    public BackendLeaseStore(DataSource dataSource) { this.dataSource = dataSource; }
    public void initialize() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS canopy_backend_leases (shard VARCHAR(128) PRIMARY KEY, instance CHAR(36) NOT NULL, seen_at TIMESTAMP(3) NOT NULL) ENGINE=InnoDB");
        }
    }
    public void claim(String shard, String instance) throws SQLException {
        PlayerSession.requireName(shard);
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(
            "INSERT INTO canopy_backend_leases (shard, instance, seen_at) VALUES (?, ?, CURRENT_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE instance=IF(seen_at <= TIMESTAMPADD(SECOND,-30,CURRENT_TIMESTAMP(3)),VALUES(instance),instance), seen_at=IF(instance=VALUES(instance),CURRENT_TIMESTAMP(3),seen_at)")) {
            s.setString(1, shard); s.setString(2, instance); s.executeUpdate();
        }
        heartbeat(shard, instance);
    }
    public void heartbeat(String shard, String instance) throws SQLException {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(
            "UPDATE canopy_backend_leases SET seen_at=CURRENT_TIMESTAMP(3) WHERE shard=? AND instance=?")) {
            s.setString(1, shard); s.setString(2, instance);
            if (s.executeUpdate() != 1) throw new SQLException("Backend instance superseded");
        }
    }
    public boolean alive(String shard) throws SQLException {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(
            "SELECT shard FROM canopy_backend_leases WHERE shard=? AND seen_at > TIMESTAMPADD(SECOND,-30,CURRENT_TIMESTAMP(3))")) {
            s.setString(1, shard); try (ResultSet rows = s.executeQuery()) { return rows.next(); }
        }
    }
    public void release(String shard, String instance) throws SQLException {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(
            "DELETE FROM canopy_backend_leases WHERE shard=? AND instance=?")) {
            s.setString(1, shard); s.setString(2, instance); s.executeUpdate();
        }
    }
}
