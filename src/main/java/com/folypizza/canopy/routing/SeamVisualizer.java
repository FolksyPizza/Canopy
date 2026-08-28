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
    private final int buffer;

    public SeamVisualizer(org.bukkit.plugin.java.JavaPlugin plugin, boolean enabled, double boundaryX, int buffer) {
        this.plugin = plugin;
        this.enabled = enabled;
        this.boundaryX = boundaryX;
        this.buffer = Math.max(0, buffer);
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
        // Draw the two edges of the inaccessible underlap band (x = boundary ± buffer) as
        // faint columns aligned to the player's z, so the marker sits exactly where the
        // crossing happens. With buffer 0 this collapses to a single line at the boundary.
        double z = loc.getBlockZ() + 0.5;
        int py = loc.getBlockY();
        double westEdge = boundaryX - buffer;
        double eastEdge = boundaryX + buffer;
        for (int y = py; y <= py + 2; y++) {
            p.spawnParticle(Particle.END_ROD, new Location(w, westEdge, y + 0.5, z), 1, 0, 0, 0, 0);
            if (buffer > 0) {
                p.spawnParticle(Particle.END_ROD, new Location(w, eastEdge, y + 0.5, z), 1, 0, 0, 0, 0);
            }
        }
    }
}
