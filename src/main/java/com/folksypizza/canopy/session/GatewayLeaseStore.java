package com.folksypizza.canopy.session;

import javax.sql.DataSource;
import java.sql.*;

/** Presence lease uses the database clock, so gateway hosts need not share a wall clock. */
public final class GatewayLeaseStore {
    private final DataSource dataSource;
    public GatewayLeaseStore(DataSource dataSource) { this.dataSource = dataSource; }
    public void initialize() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS canopy_gateway_leases (gateway_id VARCHAR(128) PRIMARY KEY, seen_at TIMESTAMP(3) NOT NULL) ENGINE=InnoDB");
        }
    }
    public void heartbeat(String gateway) throws SQLException {
        PlayerSession.requireName(gateway);
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(
                "INSERT INTO canopy_gateway_leases (gateway_id, seen_at) VALUES (?, CURRENT_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE seen_at=CURRENT_TIMESTAMP(3)")) {
            s.setString(1, gateway); s.executeUpdate();
        }
    }
    public boolean alive(String gateway) throws SQLException {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(
                "SELECT gateway_id FROM canopy_gateway_leases WHERE gateway_id=? AND seen_at > TIMESTAMPADD(SECOND,-30,CURRENT_TIMESTAMP(3))")) {
            s.setString(1, gateway);
            try (ResultSet r = s.executeQuery()) { return r.next(); }
        }
    }
}
