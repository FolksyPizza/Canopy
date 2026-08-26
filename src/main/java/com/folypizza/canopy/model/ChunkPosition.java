package com.folypizza.canopy.model;

import java.util.Objects;

/**
 * Chunk position in the world.
 */
public record ChunkPosition(int chunkX, int chunkZ) {
    public ChunkPosition {
        if (chunkX < -30000000 || chunkX > 30000000) {
            throw new IllegalArgumentException("Chunk X out of bounds: " + chunkX);
        }
        if (chunkZ < -30000000 || chunkZ > 30000000) {
            throw new IllegalArgumentException("Chunk Z out of bounds: " + chunkZ);
        }
    }

    public static ChunkPosition fromBlockCoords(int bx, int bz) {
        return new ChunkPosition(bx >> 4, bz >> 4);
    }

    public int getTileTopBlockX() {
        return chunkX << 4;
    }

    public int getTileTopBlockZ() {
        return chunkZ << 4;
    }

    /** Get which tile this chunk belongs to */
    public TilePosition toTilePosition() {
        int tileX = (chunkX >> 4) << 8; // tile = 16 chunks, each 16 blocks
        int tileZ = (chunkZ >> 4) << 8;
        return new TilePosition(tileX, tileZ);
    }
}
