package com.folksypizza.canopy.routing;

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
import com.folksypizza.canopy.migration.PlayerStateCodec.LandingMotion;

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
 *
 * <p>Exact handover (transfer.exact-handover, proxy mode, zero buffer, needs the Canopy Session Gateway): while a
 * player approaches the seam this shard streams ghost hints so the peer prepares the landing. At the crossing it only
 * asks the gateway to switch; the gateway holds the player's input and sends {@code canopy:cut} down this shard's
 * inbound stream. The snapshot taken on that marker is exact, so the peer applies it without projection, reports
 * {@code canopy:ready}, and receives the held input. See docs/exact-handover.md.
 */
public class BoundaryTransferListener implements Listener, org.bukkit.plugin.messaging.PluginMessageListener {
    private static final Logger log = LoggerFactory.getLogger(BoundaryTransferListener.class);

    /** Custom channel handled by the CanopySwitch Velocity plugin. */
    public static final String SWITCH_CHANNEL = "canopy:switch";
    /** Exact handover: gateway-to-shard input cut marker, cancelled switch, and destination ready signal. */
    public static final String CUT_CHANNEL = "canopy:cut";
    public static final String CANCEL_CHANNEL = "canopy:cut-cancel";
    public static final String READY_CHANNEL = "canopy:ready";
    /** Without a cut marker by then, the proxy does not support exact handover: fall back to projection. */
    private static final long CUT_FALLBACK_MS = 1000;
    /** A frozen source copy that never left (the switch failed silently) resumes after this. */
    private static final long FROZEN_RELEASE_MS = 12_000;

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
    private final com.folksypizza.canopy.grpc.PeerManager peerManager;

    private static final long SETTLE_MS = 1500;
    // Lead distance before the border to initiate the switch when walking toward the peer. Kept at
    // zero so the crossing position and look direction are captured at the true seam crossing and
    // preserved exactly on arrival; a non-zero lead would capture (and land) a fraction early.
    private static final double CROSS_LEAD = 0.0;

    // Players mid-transfer, so repeated move events don't fire transfer() twice.
    private final Set<UUID> transferring = ConcurrentHashMap.newKeySet();
    // When each player joined, to suppress transfer during the spawn/restore settling window.
    private final java.util.Map<UUID, Long> joinedAt = new ConcurrentHashMap<>();
    private record MovementSample(double x, double z, long atNanos, double stepX, double stepZ) {}
    private final java.util.Map<UUID, MovementSample> recentMovement = new ConcurrentHashMap<>();
    // Throttle the maintenance message per player.
    private static final long DENY_MSG_MS = 3000;
    private final java.util.Map<UUID, Long> lastDenyMsg = new ConcurrentHashMap<>();

    private final boolean exactHandover;
    private final double ghostDistance;
    private final long ghostIntervalMs;
    private final java.util.Map<UUID, Long> lastGhost = new ConcurrentHashMap<>();
    // Crossing recorded, cut marker not yet received: what the projected fallback needs.
    private record PendingCut(Location landing, LandingMotion motion) {}
    private final java.util.Map<UUID, PendingCut> pendingCut = new ConcurrentHashMap<>();
    // Players whose state was handed over at the cut: this shard's copy is stale and must not change.
    private final Set<UUID> frozen = ConcurrentHashMap.newKeySet();

    // Abuse guards (transfer.guard). A crossing costs the peer a backend join, a state push and chunk traffic, so a
    // player walking back and forth, or many players crossing at once, could lag both shards. Beyond these limits
    // the seam holds the player on this side (a soft wall) instead of handing them over.
    private final int maxCrossingsPerMinute;
    private final long minCrossingIntervalMs;
    private final int maxConcurrentHandovers;
    private final double peerMaxMspt;
    private final int maxHintsPerSecond;
    private final java.util.Map<UUID, java.util.ArrayDeque<Long>> crossings = new ConcurrentHashMap<>();
    private final Set<UUID> inFlight = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong hintWindow = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicInteger hintsInWindow = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.Map<UUID, Long> lastHoldMsg = new ConcurrentHashMap<>();

    // Fallback projection when source and destination wall clocks cannot be compared.
    private final int landingLeadTicks;
    // A cold backend join can exceed a second. Keep enough travel to cover that delay while
    // bounding projection if a switch stalls or the player changes direction mid-switch.
    private static final double MAX_LANDING_TICKS = 40.0;
    private static final double MAX_GROUND_STEP = 0.35;
    private static final double MAX_GROUND_PROJECTION = 12.0;

    public BoundaryTransferListener(JavaPlugin plugin, boolean enabled, double boundaryX, int buffer,
                                    boolean ownsWest, String mode, String peerServer,
                                    String peerHost, int peerPort,
                                    PlayerStateInbox inbox,
                                    com.folksypizza.canopy.grpc.PeerManager peerManager) {
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
        this.exactHandover = plugin.getConfig().getBoolean("transfer.exact-handover", false)
            && "proxy".equalsIgnoreCase(this.mode) && this.buffer == 0;
        this.ghostDistance = plugin.getConfig().getDouble("transfer.ghost.distance", 24.0);
        this.ghostIntervalMs = Math.max(50, plugin.getConfig().getLong("transfer.ghost.interval-ms", 100));
        GuardSettings guard = GuardSettings.from(plugin.getConfig());
        this.maxCrossingsPerMinute = guard.maxCrossingsPerMinute();
        this.minCrossingIntervalMs = guard.minIntervalMs();
        this.maxConcurrentHandovers = guard.maxConcurrent();
        this.peerMaxMspt = guard.peerMaxMspt();
        this.maxHintsPerSecond = guard.maxHintsPerSecond();
        log.info("Crossing guard: {}", guard);
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

    /** Extend a walking crossing by the time spent switching backends. */
    private Location projectLanding(Location base, LandingMotion motion) {
        if (base == null || motion == null || motion.startedAtMillis() == 0) return base;
        if (!Double.isFinite(motion.stepX()) || !Double.isFinite(motion.stepZ())) return base;
        long elapsed = System.currentTimeMillis() - motion.startedAtMillis();
        double ticks = elapsed >= 0 && elapsed <= 5_000
            ? Math.min(elapsed / 50.0, MAX_LANDING_TICKS)
            : landingLeadTicks;
        double travelX = motion.stepX() * ticks;
        double travelZ = motion.stepZ() * ticks;
        double distance = Math.hypot(travelX, travelZ);
        if (distance > MAX_GROUND_PROJECTION) {
            travelX *= MAX_GROUND_PROJECTION / distance;
            travelZ *= MAX_GROUND_PROJECTION / distance;
        }
        Location projected = base.clone().add(travelX, 0, travelZ);
        projected.setX(ownsWest
            ? Math.min(projected.getX(), boundaryX - buffer - 0.001)
            : Math.max(projected.getX(), boundaryX + buffer));
        return projected;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent e) {
        if (!enabled) return;
        Player p = e.getPlayer();
        joinedAt.put(p.getUniqueId(), System.currentTimeMillis());
        // Prefer the full state blob pushed by the source shard; it may arrive slightly
        // after join, so poll the inbox briefly before falling back to the position cookie.
        if (managedSessions() == null) tryApplyState(p, 0);
    }

    private void tryApplyState(Player p, int attempt) {
        if (!p.isOnline()) return;
        byte[] blob = inbox != null ? inbox.take(p.getUniqueId()) : null;
        if (blob != null) {
            p.getScheduler().run(plugin, task -> ((com.folksypizza.canopy.CanopyPlugin) plugin).getRespawnRepair()
                .awaitGateway(p, exactHandover).thenRun(() -> restoreIncoming(p, blob)).exceptionally(error -> {
                    log.warn("Player arrival is held because restoration failed", error);
                    return null;
                }), null);
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

    private void restoreIncoming(Player p, byte[] blob) {
        Location loc = projectLanding(
            com.folksypizza.canopy.migration.PlayerStateCodec.apply(p, blob, p.getWorld(),
                arrival -> ((com.folksypizza.canopy.CanopyPlugin) plugin).getRespawnRepair().beforeRestore(p, arrival)),
            com.folksypizza.canopy.migration.PlayerStateCodec.readLandingMotion(blob));
        org.bukkit.util.Vector vel = com.folksypizza.canopy.migration.PlayerStateCodec.readVelocity(blob);
        org.bukkit.inventory.ItemStack boost =
            com.folksypizza.canopy.migration.PlayerStateCodec.readFireworkBoost(blob);
        if (loc == null) {
            log.warn("Player arrival state was refused; no readiness acknowledgement sent");
            return;
        }
        if (loc != null) {
            if (com.folksypizza.canopy.migration.PlayerStateCodec.readLandingMotion(blob) != null) {
                log.info("Landing {} at ({},{},{})", p.getName(),
                    loc.getX(), loc.getY(), loc.getZ());
            }
            // Restore momentum after the teleport lands (a teleport clears velocity), so the
            // player keeps moving through the seam instead of stopping dead on arrival.
            p.teleportAsync(loc).thenAccept(ok -> {
                if (ok) p.getScheduler().run(plugin, tt -> {
                    if (vel != null) p.setVelocity(vel);
                    if (boost != null) {
                        p.setGliding(true);
                        p.boostElytra(boost);
                    }
                    // Exact handover: the state is in place; the gateway may replay the held input.
                    ((com.folksypizza.canopy.CanopyPlugin) plugin).getRespawnRepair()
                        .awaitGateway(p, exactHandover).thenRun(() ->
                            p.sendPluginMessage(plugin, READY_CHANNEL, new byte[0])).exceptionally(error -> {
                                log.warn("Restored arrival is held because gateway readiness could not be sent", error);
                                return null;
                            });
                }, () -> {});
            });
        }
        // Pearl teleport sound, played on arrival so it's guaranteed to reach the client
        // after the server switch (a source-side sound can be dropped as the link swaps).
        p.playSound(p.getLocation(), Sound.ENTITY_ENDERMAN_TELEPORT, 1.0f, 1.0f);
        log.info("Applied full player state for {} (arrived from peer)", p.getName());
        // An arrival counts toward the crossing budget too, so bouncing between shards is limited both ways.
        java.util.ArrayDeque<Long> recent = crossings.computeIfAbsent(p.getUniqueId(), k -> new java.util.ArrayDeque<>());
        synchronized (recent) {
            recent.addLast(System.currentTimeMillis());
        }
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
                if (parts.length >= 8) {
                    loc = projectLanding(loc, new LandingMotion(
                        Double.parseDouble(parts[5]), Double.parseDouble(parts[6]),
                        Long.parseLong(parts[7])));
                }
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
        if (frozen.contains(p.getUniqueId())) return;       // handed over; the peer owns this player now
        // Fire the instant the player crosses toward the peer. A small lead is applied only when
        // they are actually moving toward the peer, so the switch — which takes a moment to
        // complete — lands them at the seam rather than well past it. Walking parallel to (or away
        // from) the seam uses no lead, so nobody lingering near the border is yanked across.
        Location from = e.getFrom();
        double dx = to.getX() - from.getX();
        double dz = to.getZ() - from.getZ();
        long nowNanos = System.nanoTime();
        MovementSample previous = recentMovement.get(p.getUniqueId());
        double stepX = dx;
        double stepZ = dz;
        if (previous != null) {
            long elapsedNanos = nowNanos - previous.atNanos();
            if (elapsedNanos >= 20_000_000L && elapsedNanos <= 250_000_000L) {
                double scale = 50_000_000.0 / elapsedNanos;
                stepX = 0.5 * previous.stepX() + 0.5 * (to.getX() - previous.x()) * scale;
                stepZ = 0.5 * previous.stepZ() + 0.5 * (to.getZ() - previous.z()) * scale;
            } else if (elapsedNanos < 20_000_000L) {
                stepX = previous.stepX();
                stepZ = previous.stepZ();
            }
        }
        double stepLength = Math.hypot(stepX, stepZ);
        double maxStep = p.isGliding() || p.isFlying() ? 3.0 : MAX_GROUND_STEP;
        if (stepLength > maxStep) {
            stepX *= maxStep / stepLength;
            stepZ *= maxStep / stepLength;
        }
        recentMovement.put(p.getUniqueId(), new MovementSample(
            to.getX(), to.getZ(), nowNanos, stepX, stepZ));
        double lead = (ownsWest ? dx > 0 : dx < 0) ? CROSS_LEAD : 0.0;
        boolean crossed = ownsWest
            ? (to.getX() >= boundaryX - buffer - lead)
            : (to.getX() < boundaryX + buffer + lead);
        if (!crossed) {                             // still on our side
            ghostHint(p, to, stepX, stepZ);
            return;
        }
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
        if (transferring.contains(p.getUniqueId())) return; // already transferring
        String hold = crossingHold(p);
        if (hold != null) {
            holdAtSeam(e, p, to, hold);
            return;
        }

        if (!transferring.add(p.getUniqueId())) return; // already transferring
        recordCrossing(p.getUniqueId());

        // Record the exact crossing and movement per tick. The destination projects it by the
        // elapsed switch time, so a slower backend switch lands farther into the peer shard.
        double landX = buffer > 0 ? landingX() : clampLandingX(to.getX());
        Location landing = new Location(p.getWorld(), landX, to.getY(), to.getZ(),
            to.getYaw(), to.getPitch());
        log.info("Crossing {} at x={} step=({}, {})", p.getName(), to.getX(), stepX, stepZ);
        LandingMotion motion = new LandingMotion(stepX, stepZ, System.currentTimeMillis());
        if (exactHandover) {
            beginExactHandover(p, landing, motion);
        } else {
            doHandover(p, landing, motion);
        }
    }

    /**
     * Ghost phase: while a player heads for the seam within transfer.ghost.distance, tell the peer where they are
     * predicted to land so it can have that ground loaded. Rate limited per player; nothing on the peer changes
     * beyond chunk tickets, which expire if the player turns back.
     */
    private void ghostHint(Player p, Location to, double stepX, double stepZ) {
        if (!exactHandover || peerManager == null) return;
        if (crossingHold(p) != null) return;                // no point preparing a crossing that will be held
        double toSeam = ownsWest ? (boundaryX - to.getX()) : (to.getX() - boundaryX);
        boolean heading = ownsWest ? stepX > 0 : stepX < 0;
        if (toSeam > ghostDistance || !heading) return;
        long now = System.currentTimeMillis();
        Long last = lastGhost.get(p.getUniqueId());
        if (last != null && now - last < ghostIntervalMs) return;
        lastGhost.put(p.getUniqueId(), now);
        if (!takeHint(now)) return;
        double ticks = Math.min(40.0, Math.abs(toSeam / stepX));
        var hint = com.folksypizza.canopy.proto.ArrivalHint.newBuilder()
            .setUuid(p.getUniqueId().toString()).setWorld(to.getWorld().getName())
            .setX(boundaryX).setY(to.getY()).setZ(to.getZ() + stepZ * ticks).build();
        plugin.getServer().getAsyncScheduler().runNow(plugin, t -> peerManager.prepareArrival(hint));
    }

    /** Why a crossing must wait right now, or null if it may go ahead. */
    private String crossingHold(Player p) {
        long now = System.currentTimeMillis();
        java.util.ArrayDeque<Long> recent = crossings.get(p.getUniqueId());
        if (recent != null) {
            synchronized (recent) {
                while (!recent.isEmpty() && now - recent.peekFirst() > 60_000L) recent.pollFirst();
                if (!recent.isEmpty() && now - recent.peekLast() < minCrossingIntervalMs) return "You just crossed. One moment.";
                if (recent.size() >= maxCrossingsPerMinute) return "You are crossing too often. Please wait a moment.";
            }
        }
        if (inFlight.size() >= maxConcurrentHandovers) return "The other side is busy. One moment.";
        if (peerManager != null && peerMaxMspt > 0 && peerManager.getPeerMspt() > peerMaxMspt) {
            return "The other side is busy. One moment.";
        }
        return null;
    }

    private void recordCrossing(UUID id) {
        java.util.ArrayDeque<Long> recent = crossings.computeIfAbsent(id, k -> new java.util.ArrayDeque<>());
        synchronized (recent) {
            recent.addLast(System.currentTimeMillis());
        }
        inFlight.add(id);
        // A handover that never completes (the player stays here) frees its slot once the transfer flag clears.
        plugin.getServer().getAsyncScheduler().runDelayed(plugin, t -> inFlight.remove(id),
            FROZEN_RELEASE_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /** Keep the player on this side of the seam: the move is replaced by the last position on our side. */
    private void holdAtSeam(PlayerMoveEvent e, Player p, Location to, String reason) {
        Location from = e.getFrom();
        Location held = from.clone();
        held.setYaw(to.getYaw());
        held.setPitch(to.getPitch());
        if (!ownsX(held.getX())) {
            held.setX(ownsWest ? boundaryX - buffer - 0.3 : boundaryX + buffer + 0.3);
        }
        e.setTo(held);
        long now = System.currentTimeMillis();
        Long last = lastHoldMsg.get(p.getUniqueId());
        if (last == null || now - last > DENY_MSG_MS) {
            lastHoldMsg.put(p.getUniqueId(), now);
            log.info("Holding {} at the seam: {}", p.getName(), reason);
            p.sendActionBar(net.kyori.adventure.text.Component.text(reason,
                net.kyori.adventure.text.format.NamedTextColor.GRAY));
        }
    }

    /** Global ghost-hint rate: a crowd at the seam must not turn into a flood of peer chunk loads. */
    private boolean takeHint(long now) {
        long window = now / 1000L;
        if (hintWindow.getAndSet(window) != window) hintsInWindow.set(0);
        return hintsInWindow.incrementAndGet() <= maxHintsPerSecond;
    }

    /**
     * Exact handover: only ask for the switch now. The gateway answers with the cut marker (see
     * {@link #onPluginMessageReceived}); if it never does, this proxy has no exact handover and the projected handover
     * runs instead.
     */
    private void beginExactHandover(Player p, Location landing, LandingMotion motion) {
        UUID id = p.getUniqueId();
        UUID transferId = UUID.randomUUID();
        pendingCut.put(id, new PendingCut(landing, motion));
        sendSwitch(p, true, transferId);
        p.getScheduler().runDelayed(plugin, t -> {
            PendingCut pending = pendingCut.remove(id);
            if (pending != null && p.isOnline()) {
                log.info("No cut marker for {}; using the projected handover", p.getName());
                doHandover(p, pending.landing(), pending.motion());
            }
        }, () -> pendingCut.remove(id), CUT_FALLBACK_MS / 50);
    }

    @Override
    public void onPluginMessageReceived(String channel, Player p, byte[] message) {
        UUID id = p.getUniqueId();
        if (CUT_CHANNEL.equals(channel)) {
            if (managedSessions() != null) {
                pendingCut.remove(id); managedSessions().markCut(p); return;
            }
            if (pendingCut.remove(id) == null) return;
            // Every input the gateway forwarded before the marker has been processed, and nothing after it will
            // arrive: this is the player's exact state. Hand it over unprojected and freeze the local copy.
            Location exact = p.getLocation();
            byte[] blob = com.folksypizza.canopy.migration.PlayerStateCodec.serialize(p, exact, new LandingMotion(0, 0, 0));
            if (blob.length > 0 && !p.isDead() && p.getHealth() > 0) {
                // The dead destination may retire before receiving plugin registrations; prepare its repair now.
                p.sendPluginMessage(plugin, PlayerRespawnRepair.CHANNEL, new byte[]{1});
            }
            frozen.add(id);
            log.info("Exact handover of {} at ({}, {}, {})", p.getName(), exact.getX(), exact.getY(), exact.getZ());
            if (peerManager != null) {
                String uuid = id.toString();
                plugin.getServer().getAsyncScheduler().runNow(plugin, t -> peerManager.pushPlayerState(uuid, blob));
            }
            p.getScheduler().runDelayed(plugin, t -> {
                if (frozen.remove(id)) {
                    transferring.remove(id);
                    log.warn("{} was handed over but never left; resuming them here", p.getName());
                }
            }, null, FROZEN_RELEASE_MS / 50);
        } else if (CANCEL_CHANNEL.equals(channel)) {
            frozen.remove(id);
            pendingCut.remove(id);
            transferring.remove(id);
            log.info("Switch of {} cancelled by the gateway; resuming them here", p.getName());
        }
    }

    // A frozen player's state belongs to the peer now: nothing on this shard may change it.
    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = true)
    public void onFrozenDamage(org.bukkit.event.entity.EntityDamageEvent e) {
        if (e.getEntity() instanceof Player p && frozen.contains(p.getUniqueId())) e.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = true)
    public void onFrozenPickup(org.bukkit.event.entity.EntityPickupItemEvent e) {
        if (e.getEntity() instanceof Player p && frozen.contains(p.getUniqueId())) e.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = true)
    public void onFrozenDrop(org.bukkit.event.player.PlayerDropItemEvent e) {
        if (frozen.contains(e.getPlayer().getUniqueId())) e.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onFrozenInteract(org.bukkit.event.player.PlayerInteractEvent e) {
        if (frozen.contains(e.getPlayer().getUniqueId())) e.setCancelled(true);
    }

    /** Ask the gateway to switch; {@code exact} (a trailing byte older gateways ignore) requests the input cut. */
    private void sendSwitch(Player p, boolean exact) {
        sendSwitch(p, exact, null);
    }

    private void sendSwitch(Player p, boolean exact, UUID transferId) {
        com.google.common.io.ByteArrayDataOutput out = com.google.common.io.ByteStreams.newDataOutput();
        out.writeUTF(peerServer);
        out.writeByte(exact ? 1 : 0);
        if (exact && transferId != null) {
            out.writeLong(transferId.getMostSignificantBits());
            out.writeLong(transferId.getLeastSignificantBits());
        }
        log.info("Handing {} to server '{}'", p.getName(), peerServer);
        p.sendPluginMessage(plugin, SWITCH_CHANNEL, out.toByteArray());
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

    /** Transfer a player to a pearl's actual impact point on the destination shard. */
    public boolean forceHandoverAt(Player p, double x, double y, double z, boolean pearl) {
        if (!enabled || !Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(z)) return false;
        boolean onPeer = ownsWest ? x >= boundaryX + buffer : x < boundaryX - buffer;
        if (!onPeer || (peerManager != null && !peerManager.isPeerHealthy())) return false;
        if (!transferring.add(p.getUniqueId())) return false;
        if (pearl && p.getGameMode() != org.bukkit.GameMode.CREATIVE
            && p.getGameMode() != org.bukkit.GameMode.SPECTATOR) {
            p.damage(5.0);
            if (p.isDead()) {
                transferring.remove(p.getUniqueId());
                return false;
            }
        }
        Location current = p.getLocation();
        Location landing = new Location(p.getWorld(), x, y, z,
            current.getYaw(), current.getPitch());
        doHandover(p, landing);
        return true;
    }

    private com.folksypizza.canopy.session.runtime.SessionRuntime managedSessions() {
        return plugin instanceof com.folksypizza.canopy.CanopyPlugin canopy ? canopy.getSessionRuntime() : null;
    }

    /** Serialize + push state, then switch the player to the peer shard. */
    private void doHandover(Player p, Location landing) {
        doHandover(p, landing, new LandingMotion(0, 0, 0));
    }

    private void doHandover(Player p, Location landing, LandingMotion motion) {
        var managed = managedSessions();
        if (managed != null) {
            managed.landing(p, landing);
            sendSwitch(p, true);
            return;
        }
        // The teleport sound is played on the destination when state is applied (see tryApplyState),
        // where it reliably reaches the client after the server switch.
        byte[] payload = (landing.getX() + ";" + landing.getY() + ";" + landing.getZ() + ";"
            + landing.getYaw() + ";" + landing.getPitch() + ";" + motion.stepX() + ";"
            + motion.stepZ() + ";" + motion.startedAtMillis()).getBytes(StandardCharsets.UTF_8);
        try {
            p.storeCookie(cookieKey, payload);
            // Serialize full player state now (on the player's region thread) with the landing
            // position, and push it to the destination shard so it's waiting when they rejoin.
            if (peerManager != null) {
                final byte[] blob = com.folksypizza.canopy.migration.PlayerStateCodec.serialize(p, landing, motion);
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
                sendSwitch(p, false);
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
        recentMovement.remove(e.getPlayer().getUniqueId());
        frozen.remove(e.getPlayer().getUniqueId());
        pendingCut.remove(e.getPlayer().getUniqueId());
        lastGhost.remove(e.getPlayer().getUniqueId());
        inFlight.remove(e.getPlayer().getUniqueId());
        lastHoldMsg.remove(e.getPlayer().getUniqueId());
        // The crossing budget follows the player across shards only per shard; keep it briefly so quitting and
        // rejoining does not reset it.
        UUID gone = e.getPlayer().getUniqueId();
        plugin.getServer().getAsyncScheduler().runDelayed(plugin, t -> {
            if (plugin.getServer().getPlayer(gone) == null) crossings.remove(gone);
        }, 60, java.util.concurrent.TimeUnit.SECONDS);
    }
}
