package com.folypizza.canopy.migration;

import org.bukkit.ChunkSnapshot;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Captures and serializes a chunk's block-state grid for migration.
 *
 * On Folia a chunk may only be read from the region thread that owns it, so the
 * snapshot is taken via the region scheduler and marshalled back to the caller.
 * The wire format is a palette-indexed grid:
 *
 * <pre>
 *   int  minY
 *   int  maxY
 *   int  paletteSize
 *   repeated { int len; utf8[len] }   // palette entries (block-type keys)
 *   int[16 * 16 * (maxY-minY)] indices // y-major, then x, then z
 * </pre>
 */
public final class ChunkSnapshotSerializer {
    private static final Logger log = LoggerFactory.getLogger(ChunkSnapshotSerializer.class);

    private ChunkSnapshotSerializer() {}

    /**
     * Serialize the block-state grid of a loaded chunk. Returns an empty array if the
     * chunk is not loaded or the snapshot could not be captured within the timeout.
     */
    public static byte[] serialize(JavaPlugin plugin, World world, int chunkX, int chunkZ, long timeoutMs) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        plugin.getServer().getRegionScheduler().run(plugin, world, chunkX, chunkZ, task -> {
            try {
                if (!world.isChunkLoaded(chunkX, chunkZ)) {
                    future.complete(new byte[0]);
                    return;
                }
                ChunkSnapshot snapshot = world.getChunkAt(chunkX, chunkZ)
                    .getChunkSnapshot(false, false, false);
                future.complete(encode(world, snapshot));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Chunk snapshot ({},{}) in {} failed: {}", chunkX, chunkZ, world.getName(), e.getMessage());
            return new byte[0];
        }
    }

    private static byte[] encode(World world, ChunkSnapshot snapshot) throws Exception {
        int minY = world.getMinHeight();
        int maxY = world.getMaxHeight();
        int height = Math.max(0, maxY - minY);

        Map<String, Integer> palette = new LinkedHashMap<>();
        int[] grid = new int[16 * 16 * height];
        int gi = 0;
        for (int y = minY; y < maxY; y++) {
            for (int x = 0; x < 16; x++) {
                for (int z = 0; z < 16; z++) {
                    Material material = snapshot.getBlockType(x, y, z);
                    String key = material.getKey().toString();
                    Integer idx = palette.get(key);
                    if (idx == null) {
                        idx = palette.size();
                        palette.put(key, idx);
                    }
                    grid[gi++] = idx;
                }
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(grid.length * 2 + 64);
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(minY);
            out.writeInt(maxY);
            out.writeInt(palette.size());
            for (String key : palette.keySet()) {
                byte[] b = key.getBytes(StandardCharsets.UTF_8);
                out.writeInt(b.length);
                out.write(b);
            }
            for (int v : grid) {
                out.writeInt(v);
            }
            out.flush();
        }
        return baos.toByteArray();
    }
}
