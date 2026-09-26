package com.folypizza.canopy.routing;

import org.bukkit.Location;
import org.bukkit.NamespacedKey;
import org.bukkit.Sound;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Seamless cross-shard player movement.
 *
 * The world is split at a single X boundary. Each shard owns one side. When a
 * player walks off this shard's side, their exact position is stashed in a
 * client-side transfer cookie and they are handed to the peer shard via the
 * vanilla transfer packet ({@link Player#transfer}). On the receiving shard the
 * cookie is read on join and the player is teleported to that position, so the
 * two servers feel like one continuous world.
 *
 * Fresh joins with no cookie land at spawn (0,0), which is the single public
 * connection point.
 */
public class BoundaryTransferListener implements Listener {
    private static final Logger log = LoggerFactory.getLogger(BoundaryTransferListener.class);

    /** Custom channel handled by the CanopySwitch Velocity plugin. */
    public static final String SWITCH_CHANNEL = "canopy:switch";

    private final JavaPlugin plugin;
    private final boolean enabled;
    private final double boundaryX;
    private final int buffer;       // half-width of the inaccessible seam band ("underlap")
    private final boolean ownsWest; // owns the west side of the boundary
    private final String mode;      // "proxy" (Velocity server-switch) or "transfer" (transfer packet)
    private final String peerServer;// Velocity server name for proxy mode (e.g. "east")
    private final String peerHost;  // transfer-packet fallback
    private final int peerPort;
    private final NamespacedKey cookieKey;
    private final PlayerStateInbox inbox;
    private final com.folypizza.canopy.grpc.PeerManager peerManager;

    private static final long SETTLE_MS = 3000;
    // Lead distance before the border to initiate the switch when walking toward the peer. Kept at
    // zero so the crossing position and look direction are captured at the true seam crossing and
    // preserved exactly on arrival; a non-zero lead would capture (and land) a fraction early.
    private static final double CROSS_LEAD = 0.0;

    // Players mid-transfer, so repeated move events don't fire transfer() twice.
    private final Set<UUID> transferring = ConcurrentHashMap.newKeySet();
    // When each player joined, to suppress transfer during the spawn/restore settling window.
    private final java.util.Map<UUID, Long> joinedAt = new ConcurrentHashMap<>();
    // Throttle the maintenance message per player.
    private static final long DENY_MSG_MS = 3000;
    private final java.util.Map<UUID, Long> lastDenyMsg = new ConcurrentHashMap<>();

    // Ticks of movement to project the landing forward by, so a crossing lands the player where
    // their momentum would carry them during the switch rather than pinned to the boundary block.
    private final int landingLeadTicks;

    public BoundaryTransferListener(JavaPlugin plugin, boolean enabled, double boundaryX, int buffer,
                                    boolean ownsWest, String mode, String peerServer,
                                    String peerHost, int peerPort,
                                    PlayerStateInbox inbox,
                                    com.folypizza.canopy.grpc.PeerManager peerManager) {
        this.plugin = plugin;
        this.enabled = enabled;
        this.boundaryX = boundaryX;
        this.buffer = Math.max(0, buffer);
        this.ownsWest = ownsWest;
        this.mode = mode == null ? "transfer" : mode;
        this.peerServer = peerServer;
        this.peerHost = peerHost;
        this.peerPort = peerPort;
        this.cookieKey = new NamespacedKey(plugin, "transfer_pos");
        this.inbox = inbox;
        this.peerManager = peerManager;
        this.landingLeadTicks = Math.max(0, plugin.getConfig().getInt("transfer.landing-lead-ticks", 3));
    }

    /**
     * True if x is on this shard's accessible side. The band {@code [boundary-buffer,
     * boundary+buffer)} is the inaccessible "underlap" owned by neither side; a player who
     * steps into it is hopped over to the far edge on the peer shard.
     */
    private boolean ownsX(double x) {
        return ownsWest ? (x < boundaryX - buffer) : (x >= boundaryX + buffer);
    }

    /** Deterministic landing X just inside the peer's accessible zone (fixes teleport drift). */
    private double landingX() {
        return ownsWest ? (boundaryX + buffer + 0.5) : (boundaryX - buffer - 0.5);
    }

    /**
     * Clamp a live crossing X so it always lands on the peer's owned side, even when the switch was
     * initiated a hair early (the {@link #CROSS_LEAD} lead). With buffer 0 this keeps continuous 1:1
     * coordinates once the player has genuinely crossed, and pins an early initiation to the seam.
     */
    private double clampLandingX(double x) {
        return ownsWest ? Math.max(x, boundaryX) : Math.min(x, boundaryX - 0.001);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent e) {
        if (!enabled) return;
        Player p = e.getPlayer();
        joinedAt.put(p.getUniqueId(), System.currentTimeMillis());
        // Prefer the full state blob pushed by the source shard; it may arrive slightly
        // after join, so poll the inbox briefly before falling back to the position cookie.
        tryApplyState(p, 0);
    }

    private void tryApplyState(Player p, int attempt) {
        if (!p.isOnline()) return;
        byte[] blob = inbox != null ? inbox.take(p.getUniqueId()) : null;
        if (blob != null) {
            p.getScheduler().run(plugin, t -> {
                Location loc = com.folypizza.canopy.migration.PlayerStateCodec.apply(p, blob, p.getWorld());
                org.bukkit.util.Vector vel = com.folypizza.canopy.migration.PlayerStateCodec.readVelocity(blob);
                if (loc != null) {
                    // Restore momentum after the teleport lands (a teleport clears velocity), so the
                    // player keeps moving through the seam instead of stopping dead on arrival.
                    p.teleportAsync(loc).thenAccept(ok -> {
                        if (ok && vel != null) p.getScheduler().run(plugin, tt -> p.setVelocity(vel), null);
                    });
                }
                // Pearl teleport sound, played on arrival so it's guaranteed to reach the client
                // after the server switch (a source-side sound can be dropped as the link swaps).
                p.playSound(p.getLocation(), Sound.ENTITY_ENDERMAN_TELEPORT, 1.0f, 1.0f);
                log.info("Applied full player state for {} (arrived from peer)", p.getName());
            }, null);
            return;
        }
        if (attempt >= 100) { // ~1s elapsed, no blob — fall back to cookie position only
            applyCookiePosition(p);
            return;
        }
        // Poll at 10 ms so state pushed from the source shard is applied promptly after join.
        // This avoids adding a full 40 ms polling interval to the backend login/configuration time.
        plugin.getServer().getAsyncScheduler().runDelayed(plugin,
            t -> tryApplyState(p, attempt + 1), 10, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void applyCookiePosition(Player p) {
        p.retrieveCookie(cookieKey).thenAccept(bytes -> {
            if (bytes == null || bytes.length == 0) return; // fresh join — vanilla spawn
            try {
                String[] parts = new String(bytes, StandardCharsets.UTF_8).split(";");
                Location loc = new Location(p.getWorld(),
                    Double.parseDouble(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2]),
                    parts.length > 3 ? Float.parseFloat(parts[3]) : 0f,
                    parts.length > 4 ? Float.parseFloat(parts[4]) : 0f);
                p.teleportAsync(loc);
                p.playSound(loc, Sound.ENTITY_ENDERMAN_TELEPORT, 1.0f, 1.0f);
                p.storeCookie(cookieKey, new byte[0]);
                log.info("Player {} restored to {},{},{} (cookie fallback)", p.getName(),
                    (int) loc.getX(), (int) loc.getY(), (int) loc.getZ());
            } catch (Exception ex) {
                log.warn("Cookie position restore failed for {}: {}", p.getName(), ex.getMessage());
            }
        });
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onMove(PlayerMoveEvent e) {
        if (!enabled) return;
        Location to = e.getTo();
        if (to == null) return;

        Player p = e.getPlayer();
        // Fire the instant the player crosses toward the peer. A small lead is applied only when
        // they are actually moving toward the peer, so the switch — which takes a moment to
        // complete — lands them at the seam rather than well past it. Walking parallel to (or away
        // from) the seam uses no lead, so nobody lingering near the border is yanked across.
        Location from = e.getFrom();
        double dx = to.getX() - from.getX();
        double lead = (ownsWest ? dx > 0 : dx < 0) ? CROSS_LEAD : 0.0;
        boolean crossed = ownsWest
            ? (to.getX() >= boundaryX - buffer - lead)
            : (to.getX() < boundaryX + buffer + lead);
        if (!crossed) return;                       // still on our side
        // Suppress transfer during the post-join settling window (avoids spawn-jitter loops
        // while a cookie-restore teleport is still landing the player on our side).
        Long jt = joinedAt.get(p.getUniqueId());
        if (jt != null && System.currentTimeMillis() - jt < SETTLE_MS) return;

        // If the destination shard is unreachable, refuse the crossing: knock the player back
        // into our region and tell them the region is under maintenance. Only this case holds
        // the player; a healthy crossing lets them keep walking while the switch runs.
        if (peerManager != null && !peerManager.isPeerHealthy()) {
            denyCrossing(p, to);
            return;
        }

        if (!transferring.add(p.getUniqueId())) return; // already transferring

        // Project the landing forward along the player's movement by a few ticks, so with 1:1
        // continuous coordinates (buffer 0) they arrive where their momentum would have carried
        // them during the switch — not pinned to the boundary block. With a non-zero buffer the
        // player instead hops the inaccessible band to a fixed far-edge X.
        double dz = to.getZ() - from.getZ();
        double projX = to.getX() + dx * landingLeadTicks;
        double projZ = to.getZ() + dz * landingLeadTicks;
        double landX = buffer > 0 ? landingX() : clampLandingX(projX);
        double landZ = buffer > 0 ? to.getZ() : projZ;
        Location landing = new Location(p.getWorld(), landX, to.getY(), landZ,
            to.getYaw(), to.getPitch());
        doHandover(p, landing);
    }

    /**
     * Force a handover to the peer shard regardless of position (used by the admin command).
     * Returns false if disabled, no peer is reachable, or the player is already transferring.
     */
    public boolean forceHandover(Player p) {
        if (!enabled) return false;
        if (peerManager != null && !peerManager.isPeerHealthy()) return false;
        if (!transferring.add(p.getUniqueId())) return false;
        Location cur = p.getLocation();
        Location landing = new Location(p.getWorld(), landingX(), cur.getY(), cur.getZ(),
            cur.getYaw(), cur.getPitch());
        doHandover(p, landing);
        return true;
    }

    /** Serialize + push state, then switch the player to the peer shard. */
    private void doHandover(Player p, Location landing) {
        // The teleport sound is played on the destination when state is applied (see tryApplyState),
        // where it reliably reaches the client after the server switch.
        byte[] payload = (landing.getX() + ";" + landing.getY() + ";" + landing.getZ() + ";"
            + landing.getYaw() + ";" + landing.getPitch()).getBytes(StandardCharsets.UTF_8);
        try {
            p.storeCookie(cookieKey, payload);
            // Serialize full player state now (on the player's region thread) with the landing
            // position, and push it to the destination shard so it's waiting when they rejoin.
            if (peerManager != null) {
                final byte[] blob = com.folypizza.canopy.migration.PlayerStateCodec.serialize(p, landing);
                final String uuid = p.getUniqueId().toString();
                plugin.getServer().getAsyncScheduler().runNow(plugin,
                    t -> peerManager.pushPlayerState(uuid, blob));
            }
            // If the switch silently fails (e.g. peer went down between the health check and
            // now), clear the transferring flag after a short delay so the player can retry —
            // by then the health poll will have flipped and they'll be denied + knocked back.
            final UUID uid = p.getUniqueId();
            plugin.getServer().getAsyncScheduler().runDelayed(plugin,
                t -> transferring.remove(uid), 4, java.util.concurrent.TimeUnit.SECONDS);

            if ("proxy".equalsIgnoreCase(mode)) {
                // Ask the CanopySwitch Velocity plugin to move this player to the peer backend
                // via Velocity's native connection request (config-phase switch, no login screen).
                com.google.common.io.ByteArrayDataOutput out = com.google.common.io.ByteStreams.newDataOutput();
                out.writeUTF(peerServer);
                log.info("Handing {} to server '{}'", p.getName(), peerServer);
                p.sendPluginMessage(plugin, SWITCH_CHANNEL, out.toByteArray());
            } else {
                log.info("Handing {} via transfer packet to {}:{}", p.getName(), peerHost, peerPort);
                p.transfer(peerHost, peerPort);
            }
        } catch (Exception ex) {
            transferring.remove(p.getUniqueId());
            log.warn("Handover of {} failed: {}", p.getName(), ex.getMessage());
        }
    }

    /** Refuse a crossing to a down shard: knock the player back and show a maintenance notice. */
    private void denyCrossing(Player p, Location to) {
        // Land 2 blocks inside our accessible zone (past the buffer band), never in the band.
        double backX = ownsWest ? (boundaryX - buffer - 2) : (boundaryX + buffer + 2);
        Location back = new Location(p.getWorld(), backX, to.getY(), to.getZ(), to.getYaw(), to.getPitch());
        org.bukkit.util.Vector kb = new org.bukkit.util.Vector(ownsWest ? -0.6 : 0.6, 0.3, 0);
        // Same pearl cue as a real crossing, so a blocked attempt still feels like a hop.
        p.playSound(p.getLocation(), Sound.ENTITY_ENDER_PEARL_THROW, 1.0f, 0.7f);
        p.teleportAsync(back).thenAccept(ok ->
            p.getScheduler().run(plugin, t -> p.setVelocity(kb), null));

        long now = System.currentTimeMillis();
        Long last = lastDenyMsg.get(p.getUniqueId());
        if (last == null || now - last > DENY_MSG_MS) {
            lastDenyMsg.put(p.getUniqueId(), now);
            p.sendMessage(net.kyori.adventure.text.Component.text(
                "The region you are trying to connect to is currently under maintenance. "
                + "Please try again in a few minutes.",
                net.kyori.adventure.text.format.NamedTextColor.RED));
        }
    }

    @EventHandler
    public void onQuit(PlayerQuitEvent e) {
        transferring.remove(e.getPlayer().getUniqueId());
        joinedAt.remove(e.getPlayer().getUniqueId());
        lastDenyMsg.remove(e.getPlayer().getUniqueId());
    }
}
