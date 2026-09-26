package com.folypizza.canopy.routing;

import org.bukkit.Color;
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
    private static final int RANGE_BLOCKS = 16;   // show the wall within this distance
    private static final int HALF_WIDTH = 8;      // z extent to each side of the player
    private static final long PERIOD_TICKS = 10;  // redraw cadence
    private static final Particle.DustOptions RED =
        new Particle.DustOptions(Color.fromRGB(255, 45, 45), 1.6f);
    private static final Particle.DustOptions GREEN =
        new Particle.DustOptions(Color.fromRGB(60, 235, 70), 1.6f);

    private final java.util.Set<java.util.UUID> loggedOnce = java.util.concurrent.ConcurrentHashMap.newKeySet();

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
        p.getScheduler().runAtFixedRate(plugin, task -> draw(p), () -> { }, 20L, PERIOD_TICKS);
    }

    private void draw(Player p) {
        if (!p.isOnline()) return;
        Location loc = p.getLocation();
        if (Math.abs(loc.getX() - boundaryX) > RANGE_BLOCKS) return;
        World w = p.getWorld();
        // Draw a visible red/green dust curtain and add bright end-rod markers along it. The
        // secondary particle stays legible with client particle settings that make dust subtle.
        double cz = loc.getZ();
        double py = loc.getY();
        boolean first = loggedOnce.add(p.getUniqueId());
        if (first) {
            org.slf4j.LoggerFactory.getLogger(SeamVisualizer.class)
                .info("[seam] drawing seam wall for {} at x={} (player x={})",
                    p.getName(), boundaryX, String.format("%.1f", loc.getX()));
        }
        for (double z = cz - HALF_WIDTH; z <= cz + HALF_WIDTH; z += 1.0) {
            for (double y = py - 1; y <= py + 4; y += 0.75) {
                Particle.DustOptions colour = (((int) Math.floor(z) + (int) Math.floor(y)) & 1) == 0 ? RED : GREEN;
                p.spawnParticle(Particle.DUST, new Location(w, boundaryX, y, z), 1, 0, 0, 0, 0, colour);
                if ((((int) Math.floor(z) + (int) Math.floor(y)) & 1) == 0) {
                    p.spawnParticle(Particle.END_ROD, new Location(w, boundaryX, y, z), 1, 0, 0, 0, 0);
                }
            }
        }
    }
}
