package com.folypizza.canopy.routing;

import io.papermc.paper.event.entity.EntityMoveEvent;
import org.bukkit.Location;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.inventory.InventoryMoveItemEvent;
import org.bukkit.event.player.PlayerTeleportEvent;
import org.bukkit.inventory.Inventory;

/**
 * Keeps non-player entities and item transfers from crossing the seam.
 *
 * Only players are handed between shards; anything else that leaves a shard's accessible
 * side would pass into the inaccessible buffer band or the peer's territory, which this
 * process does not own. To avoid duplication and loss, such crossings are blocked here:
 *
 * - Teleports that would land off this side are cancelled. PearlTransitManager handles
 *   cross-shard pearls through a player handover after impact.
 * - Non-player entities are stopped from walking into the buffer band.
 * - Item transfers (hoppers, droppers) into the buffer band are cancelled.
 */
public class SeamContainmentListener implements Listener {

    private final org.bukkit.plugin.java.JavaPlugin plugin;
    private final boolean enabled;
    private final double boundaryX;
    private final int buffer;
    private final boolean ownsWest;

    public SeamContainmentListener(org.bukkit.plugin.java.JavaPlugin plugin, boolean enabled,
                                   double boundaryX, int buffer, boolean ownsWest) {
        this.plugin = plugin;
        this.enabled = enabled;
        this.boundaryX = boundaryX;
        this.buffer = Math.max(0, buffer);
        this.ownsWest = ownsWest;
    }

    /** True if x is on this shard's accessible side (outside the buffer band). */
    private boolean owns(double x) {
        return ownsWest ? (x < boundaryX - buffer) : (x >= boundaryX + buffer);
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onTeleport(PlayerTeleportEvent e) {
        if (!enabled || e.getTo() == null) return;
        // Never allow a teleport to land past our border (pearl, chorus, or otherwise) — the
        // player can only leave via a handover, which lands them on the owned side. Cause is
        // not checked because it varies by build; the destination test is authoritative.
        if (!owns(e.getTo().getX())) {
            e.setCancelled(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onEntityMove(EntityMoveEvent e) {
        if (!enabled || !e.hasChangedBlock()) return;
        if (e.getEntity() instanceof Player) return; // players use the handover path
        if (e.getEntity() instanceof org.bukkit.entity.EnderPearl) return; // handled by PearlTransitManager
        if (!owns(e.getTo().getX())) {
            // Non-player entity trying to enter the buffer band / peer side — stop it.
            e.setCancelled(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent e) {
        if (!enabled) return;
        if (!owns(e.getBlock().getX())) {
            // No building past the border this shard owns (the buffer band or peer territory).
            e.setCancelled(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent e) {
        if (!enabled) return;
        if (!owns(e.getBlock().getX())) {
            e.setCancelled(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onItemMove(InventoryMoveItemEvent e) {
        if (!enabled) return;
        Inventory dest = e.getDestination();
        Location loc = dest.getLocation();
        if (loc != null && !owns(loc.getX())) {
            // Hopper/dropper trying to push items into the buffer band or peer side.
            e.setCancelled(true);
        }
    }
}
