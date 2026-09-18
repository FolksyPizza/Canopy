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
    private static final int RANGE_BLOCKS = 12;   // show the wall within this distance
    private static final int HALF_WIDTH = 8;      // z extent to each side of the player
    private static final long PERIOD_TICKS = 8;   // redraw cadence
    private static final Particle.DustOptions RED =
        new Particle.DustOptions(Color.fromRGB(255, 45, 45), 1.0f);
    private static final Particle.DustOptions GREEN =
        new Particle.DustOptions(Color.fromRGB(60, 235, 70), 1.0f);

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
        // A wall of red/green dust on the seam plane (x = boundary), spanning a block or two
        // to each side of the player in z and a few blocks in height, so the border reads as
        // a coloured curtain. Colours alternate per cell for the mixed look.
        int cz = loc.getBlockZ();
        int py = loc.getBlockY();
        for (int z = cz - HALF_WIDTH; z <= cz + HALF_WIDTH; z++) {
            for (int y = py - 1; y <= py + 5; y++) {
                Particle.DustOptions colour = ((z + y) & 1) == 0 ? RED : GREEN;
                p.spawnParticle(Particle.DUST, new Location(w, boundaryX, y + 0.5, z + 0.5),
                    1, 0, 0, 0, 0, colour);
            }
        }
    }
}
