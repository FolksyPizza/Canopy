package com.folypizza.canopy.repartition;

public final class RepartitionValidation {
    public static final int CHUNK_SIZE = 16;

    private RepartitionValidation() {}

    public static boolean isChunkAligned(int coordinate) {
        return Math.floorMod(coordinate, CHUNK_SIZE) == 0;
    }

    public static void requireChunkAligned(int coordinate, String label) {
        if (!isChunkAligned(coordinate)) {
            throw new IllegalArgumentException(label + " must be chunk-aligned (multiple of 16): " + coordinate);
        }
    }

    public static int affectedChunkCount(int oldBoundary, int newBoundary, int minZ, int maxZ) {
        requireChunkAligned(oldBoundary, "old boundary");
        requireChunkAligned(newBoundary, "new boundary");
        requireChunkAligned(minZ, "min-z");
        requireChunkAligned(maxZ, "max-z");
        if (oldBoundary == newBoundary || minZ >= maxZ) return 0;
        long x = Math.abs((long) newBoundary - oldBoundary) / CHUNK_SIZE;
        long z = ((long) maxZ - minZ) / CHUNK_SIZE;
        long total = x * z;
        if (total > Integer.MAX_VALUE) throw new IllegalArgumentException("affected chunk count is too large");
        return (int) total;
    }
}
