package com.folypizza.canopy.world;

import com.folypizza.canopy.entity.EntityTracker;
import com.folypizza.canopy.grpc.TileVersionServiceImpl;
import com.folypizza.canopy.model.TilePosition;
import com.folypizza.canopy.routing.PlayerRoutingProxy;
import org.bukkit.Location;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockFormEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkUnloadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bridges live Bukkit/Folia world events into Canopy's tracking systems.
 *
 * This is the integration point that actually feeds the rest of the plugin:
 * block changes bump tile versions (driving halo invalidation), chunk
 * load/unload maintain a loaded-chunk count for telemetry, and player
 * join/move/quit populate the entity tracker and routing proxy.
 *
 * All handlers run on the region thread that owns the affected chunk (Folia),
 * so reading block/entity/player state here is thread-safe. Handlers are
 * defensive: an unowned tile or out-of-range coordinate is normal and must
 * never throw out of an event callback.
 */
public class WorldEventListener implements Listener {
    private static final Logger log = LoggerFactory.getLogger(WorldEventListener.class);

    private final TileVersionServiceImpl tileVersionService;
    private final WorldPartitionAdapter worldAdapter;
    private final EntityTracker entityTracker;
    private final PlayerRoutingProxy routingProxy;
    private final long localShardId;
    private final com.folypizza.canopy.halo.HaloEditStore haloEditStore;
    private final double boundaryX;
    private final int haloWidth;
    private final boolean ownsWest;

    private final AtomicInteger loadedChunks = new AtomicInteger(0);

    public WorldEventListener(TileVersionServiceImpl tileVersionService,
                              WorldPartitionAdapter worldAdapter,
                              EntityTracker entityTracker,
                              PlayerRoutingProxy routingProxy,
                              long localShardId,
                              com.folypizza.canopy.halo.HaloEditStore haloEditStore,
                              double boundaryX, int haloWidth, boolean ownsWest) {
        this.tileVersionService = tileVersionService;
        this.worldAdapter = worldAdapter;
        this.entityTracker = entityTracker;
        this.routingProxy = routingProxy;
        this.localShardId = localShardId;
        this.haloEditStore = haloEditStore;
        this.boundaryX = boundaryX;
        this.haloWidth = haloWidth;
        this.ownsWest = ownsWest;
    }

    /** True if x is in this shard's near-boundary strip (the region peers mirror). */
    private boolean inMyStrip(int x) {
        if (haloEditStore == null || haloWidth <= 0) return false;
        return ownsWest ? (x >= boundaryX - haloWidth && x < boundaryX)
                        : (x >= boundaryX && x < boundaryX + haloWidth);
    }

    private void recordEdit(org.bukkit.block.Block b, String blockData) {
        boolean in = inMyStrip(b.getX());
        log.info("[halo] block edit at x={} y={} z={} inStrip={} (strip owns={} boundary={} halo={})",
            b.getX(), b.getY(), b.getZ(), in, ownsWest ? "west" : "east", boundaryX, haloWidth);
        if (in) {
            haloEditStore.record(b.getX(), b.getY(), b.getZ(), blockData);
        }
    }

    /** Loaded-chunk count maintained from chunk events (safe to read from any thread). */
    public int getLoadedChunkCount() {
        return Math.max(0, loadedChunks.get());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent e) {
        onBlockChanged(e.getBlock());
        recordEdit(e.getBlock(), e.getBlockPlaced().getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent e) {
        onBlockChanged(e.getBlock());
        recordEdit(e.getBlock(), "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockForm(BlockFormEvent e) {
        onBlockChanged(e.getBlock());
        recordEdit(e.getBlock(), e.getNewState().getBlockData().getAsString());
    }

    private void onBlockChanged(Block block) {
        try {
            TilePosition tile = TilePosition.fromBlockCoords(block.getX(), block.getZ());
            // Bumps the tile version store and the update tracker for halo pulls.
            tileVersionService.recordUpdate(tile);
        } catch (Exception ex) {
            log.debug("Ignored block change at {},{}: {}", block.getX(), block.getZ(), ex.getMessage());
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onChunkLoad(ChunkLoadEvent e) {
        loadedChunks.incrementAndGet();
        try {
            worldAdapter.onChunkLoaded(e.getChunk().getX(), e.getChunk().getZ(), e.getWorld().getName());
        } catch (Exception ignored) { }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onChunkUnload(ChunkUnloadEvent e) {
        loadedChunks.updateAndGet(v -> v > 0 ? v - 1 : 0);
        try {
            worldAdapter.onChunkUnloaded(e.getChunk().getX(), e.getChunk().getZ(), e.getWorld().getName());
        } catch (Exception ignored) { }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerJoin(PlayerJoinEvent e) {
        Player p = e.getPlayer();
        Location loc = p.getLocation();
        entityTracker.trackEntity(p.getUniqueId(), loc.getX(), loc.getY(), loc.getZ(),
            loc.getYaw(), loc.getPitch(), "player");
        routingProxy.registerShard(localShardId, "localhost", 25565, true);
        routingProxy.routePlayer(p.getUniqueId(), loc.getX(), loc.getZ(), p.getWorld().getName());
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerQuit(PlayerQuitEvent e) {
        entityTracker.untrackEntity(e.getPlayer().getUniqueId());
        routingProxy.unroutePlayer(e.getPlayer().getUniqueId());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerMove(PlayerMoveEvent e) {
        // PlayerMoveEvent fires very frequently; only act when the player crosses a block.
        Location from = e.getFrom();
        Location to = e.getTo();
        if (to == null) return;
        if (from.getBlockX() == to.getBlockX() && from.getBlockZ() == to.getBlockZ()) {
            return;
        }
        Player p = e.getPlayer();
        entityTracker.announcePosition(p.getUniqueId(), to.getX(), to.getY(), to.getZ(),
            to.getYaw(), to.getPitch());
        routingProxy.updatePlayerPosition(p.getUniqueId(), to.getX(), to.getZ(), p.getWorld().getName());
    }
}
