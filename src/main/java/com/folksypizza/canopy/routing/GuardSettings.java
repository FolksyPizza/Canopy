package com.folksypizza.canopy.routing;

import org.bukkit.configuration.ConfigurationSection;

import java.util.Locale;

/**
 * Abuse-guard limits for seam crossings, ghost hints and pearl relays, from a profile (transfer.guard.profile) with
 * per-key overrides. Size and memory bounds (state push size, gateway chunk view) are not part of a profile: they
 * protect the server rather than police players, and always apply.
 *
 * <pre>
 * profile   crossings/min  min interval  in flight  peer mspt  hints/s  prepared  pearl cooldown  pearls/s
 * off       unlimited      none          unlimited  off        unlimited 256      none            unlimited
 * relaxed   20             750 ms        32         60         500      128       250 ms          60
 * balanced  8              2000 ms       8          45         200      64        1000 ms         20
 * strict    4              4000 ms       4          40         100      32        2000 ms         10
 * </pre>
 */
public record GuardSettings(String profile, int maxCrossingsPerMinute, long minIntervalMs, int maxConcurrent,
                            double peerMaxMspt, int maxHintsPerSecond, int maxPrepared, long pearlCooldownMs,
                            int maxPearlsPerSecond) {

    private static final int UNLIMITED = Integer.MAX_VALUE;

    public static GuardSettings from(ConfigurationSection root) {
        ConfigurationSection g = root == null ? null : root.getConfigurationSection("transfer.guard");
        String profile = g == null ? "relaxed" : g.getString("profile", "relaxed").toLowerCase(Locale.ROOT);
        GuardSettings base = switch (profile) {
            case "off" -> new GuardSettings("off", UNLIMITED, 0, UNLIMITED, 0, UNLIMITED, 256, 0, UNLIMITED);
            case "balanced" -> new GuardSettings("balanced", 8, 2000, 8, 45, 200, 64, 1000, 20);
            case "strict" -> new GuardSettings("strict", 4, 4000, 4, 40, 100, 32, 2000, 10);
            default -> new GuardSettings("relaxed", 20, 750, 32, 60, 500, 128, 250, 60);
        };
        if (g == null) return base;
        ConfigurationSection ghost = root.getConfigurationSection("transfer.ghost");
        return new GuardSettings(base.profile(),
            Math.max(1, g.getInt("max-crossings-per-minute", base.maxCrossingsPerMinute())),
            Math.max(0, g.getLong("min-interval-ms", base.minIntervalMs())),
            Math.max(1, g.getInt("max-concurrent", base.maxConcurrent())),
            Math.max(0, g.getDouble("peer-max-mspt", base.peerMaxMspt())),
            Math.max(1, g.getInt("max-hints-per-second", base.maxHintsPerSecond())),
            Math.max(1, ghost == null ? base.maxPrepared() : ghost.getInt("max-prepared", base.maxPrepared())),
            Math.max(0, g.getLong("pearl-cooldown-ms", base.pearlCooldownMs())),
            Math.max(1, g.getInt("max-pearls-per-second", base.maxPearlsPerSecond())));
    }

    @Override
    public String toString() {
        return profile + " (crossings " + limit(maxCrossingsPerMinute) + "/min, " + minIntervalMs + " ms apart, "
            + limit(maxConcurrent) + " in flight, peer mspt " + (peerMaxMspt > 0 ? peerMaxMspt : "off") + ", hints "
            + limit(maxHintsPerSecond) + "/s, " + maxPrepared + " prepared, pearls " + pearlCooldownMs + " ms apart and "
            + limit(maxPearlsPerSecond) + "/s)";
    }

    private static String limit(int v) {
        return v == UNLIMITED ? "unlimited" : Integer.toString(v);
    }
}
