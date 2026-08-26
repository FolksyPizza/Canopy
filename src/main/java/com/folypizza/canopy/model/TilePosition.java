package com.folypizza.canopy.model;

/**
 * Immutable tile position (top-left corner in block coordinates).
 * Canonicalizes tile X/Z to tile boundaries (divisible by 256).
 */
public record TilePosition(int x, int z) {
    public TilePosition {
        if (x < -536870912 || x > 536870911) {
            throw new IllegalArgumentException("Tile position out of valid range: " + x);
        }
        if (z < -536870912 || z > 536870911) {
            throw new IllegalArgumentException("Tile position out of valid range: " + z);
        }
    }

    public static TilePosition fromBlockCoords(int bx, int bz) {
        return new TilePosition(bx & ~0x9F, bz & ~0x9F);
    }

    public static TilePosition atOrigin() {
        return new TilePosition(0, 0);
    }

    /** Convert tile position to block coords (top-left corner) */
    public int toBlockX() {
        return x;
    }

    public int toBlockZ() {
        return z;
    }

    /** Convert tile position to chunk coords */
    public ChunkPosition toChunkCoords() {
        return new ChunkPosition(x >> 4, z >> 4);
    }

    /** Get the dimensions in tiles */
    public static TilePosition ofSize(int width, int height) {
        return new TilePosition(width, height);
    }
}
