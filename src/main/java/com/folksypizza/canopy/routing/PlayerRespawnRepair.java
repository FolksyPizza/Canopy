package com.folksypizza.canopy.routing;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.bukkit.Location;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerRespawnEvent;
import org.bukkit.plugin.java.JavaPlugin;

/** Repairs a dead backend copy only after an incoming alive snapshot has been decoded. */
public final class PlayerRespawnRepair implements Listener {
    public static final String CHANNEL = "canopy:respawn-repair";
    private record Repair(UUID player, Location arrival) {}
    private final ThreadLocal<Repair> active = new ThreadLocal<>();
    private final JavaPlugin plugin;

    public PlayerRespawnRepair(JavaPlugin plugin) { this.plugin = plugin; }

    /** Completion runs on the player's entity scheduler; registration can follow backend join by a tick. */
    public CompletableFuture<Void> awaitGateway(Player player, boolean exactHandover) {
        CompletableFuture<Void> ready = new CompletableFuture<>();
        awaitGateway(player, exactHandover, ready, 40);
        return ready.orTimeout(3, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void awaitGateway(Player player, boolean exactHandover, CompletableFuture<Void> ready, int remaining) {
        if (ready.isDone()) return;
        if (!player.isOnline()) {
            ready.completeExceptionally(new IllegalStateException("Arrival player departed"));
        } else if (!exactHandover || player.isDead()
                || player.getListeningPluginChannels().contains(BoundaryTransferListener.READY_CHANNEL)) {
            // Repair a dead native copy before its entity scheduler retires. The live source pre-arms CSG at the cut.
            ready.complete(null);
        } else if (remaining == 0) {
            ready.completeExceptionally(new IllegalStateException("Gateway arrival channels were not registered"));
        } else if (player.getScheduler().runDelayed(plugin,
                task -> awaitGateway(player, true, ready, remaining - 1),
                () -> ready.completeExceptionally(new IllegalStateException("Arrival player retired")), 1) == null) {
            ready.completeExceptionally(new IllegalStateException("Arrival scheduling was refused"));
        }
    }

    /** Called on the player's entity scheduler, before applying the decoded snapshot. */
    public void beforeRestore(Player player, Location arrival) {
        if (!player.isDead()) return;
        if (!player.isOnline() || player.getHealth() > 0 || active.get() != null) {
            throw new IllegalStateException("Player cannot complete stale-death recovery");
        }
        active.set(new Repair(player.getUniqueId(), arrival.clone()));
        try {
            // The gateway consumes this marker only on the current, held destination attachment.
            player.sendPluginMessage(plugin, CHANNEL, new byte[0]);
            player.spigot().respawn();
            if (player.isDead()) throw new IllegalStateException("Native respawn did not complete");
            plugin.getLogger().info("Repaired a stale dead backend player before state restoration");
        } finally {
            active.remove();
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void respawn(PlayerRespawnEvent event) {
        Repair repair = active.get();
        if (repair != null && repair.player().equals(event.getPlayer().getUniqueId())
                && event.getRespawnReason() == PlayerRespawnEvent.RespawnReason.PLUGIN) {
            // A stale local bed/spawn must not redirect an already resolved teleport or handoff.
            event.setRespawnLocation(repair.arrival().clone());
        }
    }
}
