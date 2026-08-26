package com.folypizza.canopy.routing;

import org.bukkit.Location;
import org.bukkit.Particle;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;

/**
 * Draws a shimmering particle wall along the seam plane (x = boundary) for any player
 * standing near it, so the otherwise-invisible shard boundary is locatable in-world.
 *
 * Particles are sent per-player from that player's entity scheduler (Folia-safe), so no
 * cross-region access occurs. Purely cosmetic and client-side.
 */
public class SeamVisualizer implements Listener {
    private static final int RANGE_BLOCKS = 6;    // only show the marker when very close
    private static final long PERIOD_TICKS = 10;  // redraw cadence

    private final org.bukkit.plugin.java.JavaPlugin plugin;
    private final boolean enabled;
    private final double boundaryX;

    public SeamVisualizer(org.bukkit.plugin.java.JavaPlugin plugin, boolean enabled, double boundaryX) {
        this.plugin = plugin;
        this.enabled = enabled;
        this.boundaryX = boundaryX;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent e) {
        if (!enabled) return;
        Player p = e.getPlayer();
        p.getScheduler().runAtFixedRate(plugin, task -> draw(p), null, 20L, PERIOD_TICKS);
    }

    private void draw(Player p) {
        if (!p.isOnline()) return;
        Location loc = p.getLocation();
        if (Math.abs(loc.getX() - boundaryX) > RANGE_BLOCKS) return;
        World w = p.getWorld();
        // Subtle single-block-wide column at the seam, aligned to the player's z, a few
        // blocks tall — a faint marker that follows you along the boundary.
        double z = loc.getBlockZ() + 0.5;
        int py = loc.getBlockY();
        for (int y = py; y <= py + 2; y++) {
            p.spawnParticle(Particle.END_ROD, new Location(w, boundaryX, y + 0.5, z), 1, 0, 0, 0, 0);
        }
    }
}
