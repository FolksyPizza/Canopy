package com.folksypizza.canopy.session;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;

/** Chunk sets permit irregular partitions. Bootstrap writes never replace a newer committed generation. */
public final class JdbcOwnershipDirectory {
    private final DataSource database;
    public JdbcOwnershipDirectory(DataSource database) { this.database = database; }
    public void initialize() throws SQLException {
        try (var c = database.getConnection(); var s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS canopy_chunk_owners (world VARCHAR(128) NOT NULL, chunk_x INT NOT NULL, chunk_z INT NOT NULL, owner VARCHAR(128) NOT NULL, generation BIGINT NOT NULL, PRIMARY KEY(world,chunk_x,chunk_z)) ENGINE=InnoDB");
        }
    }
    public Optional<String> owner(String world, int chunkX, int chunkZ) throws SQLException {
        try (var c = database.getConnection(); var s = c.prepareStatement("SELECT owner FROM canopy_chunk_owners WHERE world=? AND chunk_x=? AND chunk_z=?")) {
            s.setString(1, world); s.setInt(2, chunkX); s.setInt(3, chunkZ);
            try (var row = s.executeQuery()) { return row.next() ? Optional.of(row.getString(1)) : Optional.empty(); }
        }
    }
    public void seed(String world, int chunkX, int chunkZ, String owner, long generation) throws SQLException {
        PlayerSession.requireName(world); PlayerSession.requireName(owner);
        if (generation < 1) throw new IllegalArgumentException("Invalid ownership generation");
        try (var c = database.getConnection(); var s = c.prepareStatement("INSERT IGNORE INTO canopy_chunk_owners(world,chunk_x,chunk_z,owner,generation) VALUES(?,?,?,?,?)")) {
            s.setString(1, world); s.setInt(2, chunkX); s.setInt(3, chunkZ); s.setString(4, owner); s.setLong(5, generation); s.executeUpdate();
        }
        if (!owner(world, chunkX, chunkZ).orElseThrow().equals(owner)) throw new SQLException("Conflicting chunk ownership bootstrap");
    }
}
