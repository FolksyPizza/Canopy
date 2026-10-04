package com.folksypizza.canopy.routing;

import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Destination side of the ghost phase: keeps the chunks around a player's predicted landing loaded while they approach
 * the seam from the peer shard, so the handover does not wait on chunk loading or generation.
 *
 * <p>Hints arrive over gRPC from the source shard every few ticks while the player is near the seam. Each hint moves a
 * small square of plugin chunk tickets to the latest predicted landing. When hints stop (the player turned back or has
 * arrived and holds their own tickets) the tickets are released after {@code expireMillis}. Hints never create a player
 * or touch game state, so a crossing that does not happen leaves nothing behind.
 */
public class ArrivalPrep {
    private static final Logger log = LoggerFactory.getLogger(ArrivalPrep.class);

    private record Prepared(String world, Set<Long> chunks, long lastHint) {}

    private final JavaPlugin plugin;
    private final int radius;
    private final long expireMillis;
    private final Map<UUID, Prepared> prepared = new ConcurrentHashMap<>();

    private final int maxPrepared;

    public ArrivalPrep(JavaPlugin plugin, int radiusChunks, long expireMillis, int maxPrepared) {
        this.plugin = plugin;
        this.radius = Math.max(0, Math.min(8, radiusChunks));
        this.expireMillis = Math.max(500L, expireMillis);
        this.maxPrepared = Math.max(1, maxPrepared);
    }

    public void start() {
        plugin.getServer().getAsyncScheduler().runAtFixedRate(plugin, t -> expire(), 1, 1, TimeUnit.SECONDS);
    }

    /** A predicted landing for {@code id}: hold tickets around it and release the ones it no longer covers. */
    public void hint(UUID id, String worldName, double x, double z) {
        World world = Bukkit.getWorld(worldName);
        if (world == null || !Double.isFinite(x) || !Double.isFinite(z)) return;
        // A crowd at the seam must not pin an unbounded number of chunks: beyond the cap, new arrivals are not
        // prepared (they still cross, just without the preload).
        if (!prepared.containsKey(id) && prepared.size() >= maxPrepared) return;
        int cx = (int) Math.floor(x) >> 4, cz = (int) Math.floor(z) >> 4;
        Set<Long> wanted = new HashSet<>();
        for (int dx = -radius; dx <= radius; dx++) {
            for (int dz = -radius; dz <= radius; dz++) wanted.add(key(cx + dx, cz + dz));
        }
        Prepared previous = prepared.put(id, new Prepared(worldName, wanted, System.currentTimeMillis()));
        Set<Long> add = new HashSet<>(wanted);
        Set<Long> remove = new HashSet<>();
        if (previous != null && previous.world().equals(worldName)) {
            add.removeAll(previous.chunks());
            remove.addAll(previous.chunks());
            remove.removeAll(wanted);
        } else if (previous != null) {
            release(previous);
        }
        for (long k : add) ticket(world, k, true);
        for (long k : remove) ticket(world, k, false);
        if (previous == null) {
            log.info("Preparing arrival for {} around chunk ({}, {})", id, cx, cz);
        }
    }

    private void expire() {
        long now = System.currentTimeMillis();
        prepared.entrySet().removeIf(e -> {
            if (now - e.getValue().lastHint() < expireMillis) return false;
            release(e.getValue());
            return true;
        });
    }

    public void stop() {
        prepared.values().forEach(this::release);
        prepared.clear();
    }

    private void release(Prepared p) {
        World world = Bukkit.getWorld(p.world());
        if (world == null) return;
        for (long k : p.chunks()) ticket(world, k, false);
    }

    private void ticket(World world, long k, boolean add) {
        int cx = (int) (k >> 32), cz = (int) k;
        // Region scheduler: the owning region on Folia, the main thread on Paper.
        Bukkit.getRegionScheduler().execute(plugin, world, cx, cz, () -> {
            // A hint never generates terrain: only chunks that already exist are kept loaded.
            if (add) {
                if (world.isChunkGenerated(cx, cz)) world.addPluginChunkTicket(cx, cz, plugin);
            } else {
                world.removePluginChunkTicket(cx, cz, plugin);
            }
        });
    }

    private static long key(int cx, int cz) {
        return ((long) cx << 32) | (cz & 0xffffffffL);
    }
}
