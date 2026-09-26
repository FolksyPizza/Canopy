package com.folypizza.canopy.routing;

import com.folypizza.canopy.grpc.PeerManager;
import com.folypizza.canopy.proto.PearlFlight;
import com.folypizza.canopy.proto.PearlImpact;
import io.papermc.paper.event.entity.EntityMoveEvent;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.NamespacedKey;
import org.bukkit.World;
import org.bukkit.entity.EnderPearl;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.ProjectileHitEvent;
import org.bukkit.event.entity.ProjectileLaunchEvent;
import org.bukkit.persistence.PersistentDataType;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Continues a thrown pearl on the peer shard and transfers its thrower when it lands. */
public class PearlTransitManager implements Listener {
    private static final Logger log = LoggerFactory.getLogger(PearlTransitManager.class);
    private static final long FLIGHT_TIMEOUT_SECONDS = 30;

    private record Pending(UUID owner, String world) {}

    private final JavaPlugin plugin;
    private final PeerManager peers;
    private final boolean enabled;
    private final double boundaryX;
    private final int buffer;
    private final boolean ownsWest;
    private final NamespacedKey transitIdKey;
    private final NamespacedKey transitOwnerKey;
    private final Map<UUID, Pending> pending = new ConcurrentHashMap<>();
    private final Set<UUID> relayedEntities = ConcurrentHashMap.newKeySet();
    private volatile BoundaryTransferListener transfers;

    public PearlTransitManager(JavaPlugin plugin, PeerManager peers, boolean enabled,
                               double boundaryX, int buffer, boolean ownsWest) {
        this.plugin = plugin;
        this.peers = peers;
        this.enabled = enabled;
        this.boundaryX = boundaryX;
        this.buffer = Math.max(0, buffer);
        this.ownsWest = ownsWest;
        this.transitIdKey = new NamespacedKey(plugin, "transit_pearl_id");
        this.transitOwnerKey = new NamespacedKey(plugin, "transit_pearl_owner");
    }

    public void setTransfers(BoundaryTransferListener transfers) {
        this.transfers = transfers;
    }

    private boolean owns(double x) {
        return ownsWest ? x < boundaryX - buffer : x >= boundaryX + buffer;
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onMove(EntityMoveEvent event) {
        if (!enabled || !event.hasChangedBlock() || !(event.getEntity() instanceof EnderPearl pearl)) return;
        if (!owns(event.getTo().getX())) relayOrRemove(pearl, event.getTo());
    }

    // EntityMoveEvent is not guaranteed for every projectile step on Folia. This also catches
    // a pearl crossing within a tick before it can teleport its thrower on the source shard.
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onLaunch(ProjectileLaunchEvent event) {
        if (!enabled || !(event.getEntity() instanceof EnderPearl pearl)) return;
        if (pearl.getShooter() instanceof Player owner
            && Math.abs(owner.getLocation().getX() - boundaryX) < 16) {
            Vector launch = pearl.getVelocity();
            log.info("Pearl launched by {} near seam at x={} velocity=({},{},{})",
                owner.getName(), pearl.getLocation().getX(),
                launch.getX(), launch.getY(), launch.getZ());
        }
        pearl.getScheduler().runAtFixedRate(plugin, task -> {
            if (!pearl.isValid() || pearl.isDead()) {
                task.cancel();
            } else if (!owns(pearl.getLocation().getX())) {
                relayOrRemove(pearl, pearl.getLocation());
                task.cancel();
            }
        }, () -> {}, 1L, 1L);
    }

    private void relayOrRemove(EnderPearl pearl, Location crossing) {
        UUID entityId = pearl.getUniqueId();
        if (!relayedEntities.add(entityId)) return;
        plugin.getServer().getAsyncScheduler().runDelayed(plugin,
            task -> relayedEntities.remove(entityId), FLIGHT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // A relayed pearl has no local shooter. If it returns across the border, contain it.
        if (!(pearl.getShooter() instanceof Player owner) || peers == null || !peers.isPeerHealthy()) {
            pearl.remove();
            return;
        }

        UUID id = UUID.randomUUID();
        UUID ownerId = owner.getUniqueId();
        Vector velocity = pearl.getVelocity();
        pending.put(id, new Pending(ownerId, pearl.getWorld().getName()));
        plugin.getServer().getAsyncScheduler().runDelayed(plugin,
            task -> pending.remove(id), FLIGHT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        PearlFlight flight = PearlFlight.newBuilder()
            .setId(id.toString()).setOwnerUuid(ownerId.toString())
            .setWorld(pearl.getWorld().getName())
            .setX(crossing.getX()).setY(crossing.getY()).setZ(crossing.getZ())
            .setVx(velocity.getX()).setVy(velocity.getY()).setVz(velocity.getZ()).build();
        log.info("Relaying pearl {} for {} at x={} velocity=({},{},{})",
            id, owner.getName(), crossing.getX(), velocity.getX(), velocity.getY(), velocity.getZ());
        pearl.remove();
        plugin.getServer().getAsyncScheduler().runNow(plugin, task -> {
            if (!peers.relayPearl(flight)) {
                pending.remove(id);
                log.warn("Could not relay pearl {} for {}", id, ownerId);
            }
        });
    }

    /** Called from the peer's gRPC thread; schedules the pearl on its owning region. */
    public boolean acceptFlight(PearlFlight flight) {
        if (!enabled || !valid(flight.getX(), flight.getY(), flight.getZ())
            || !valid(flight.getVx(), flight.getVy(), flight.getVz())) return false;
        try {
            UUID.fromString(flight.getId());
            UUID.fromString(flight.getOwnerUuid());
        } catch (IllegalArgumentException e) {
            return false;
        }
        double x = ownsWest
            ? Math.min(flight.getX(), boundaryX - buffer - 0.05)
            : Math.max(flight.getX(), boundaryX + buffer + 0.05);
        plugin.getServer().getGlobalRegionScheduler().run(plugin, task -> {
            World world = Bukkit.getWorld(flight.getWorld());
            if (world == null) {
                log.warn("Cannot relay pearl {}: world {} is unavailable", flight.getId(), flight.getWorld());
                return;
            }
            int chunkX = ((int) Math.floor(x)) >> 4;
            int chunkZ = ((int) Math.floor(flight.getZ())) >> 4;
            plugin.getServer().getRegionScheduler().run(plugin, world, chunkX, chunkZ, regionTask -> {
                Location at = new Location(world, x, flight.getY(), flight.getZ());
                world.spawn(at, EnderPearl.class, pearl -> {
                    pearl.getPersistentDataContainer().set(transitIdKey, PersistentDataType.STRING, flight.getId());
                    pearl.getPersistentDataContainer().set(transitOwnerKey, PersistentDataType.STRING,
                        flight.getOwnerUuid());
                    pearl.setVelocity(new Vector(flight.getVx(), flight.getVy(), flight.getVz()));
                });
                log.info("Continued pearl {} for {} at x={}", flight.getId(), flight.getOwnerUuid(), x);
            });
        });
        return true;
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onHit(ProjectileHitEvent event) {
        if (!enabled || !(event.getEntity() instanceof EnderPearl pearl)) return;
        String id = pearl.getPersistentDataContainer().get(transitIdKey, PersistentDataType.STRING);
        String owner = pearl.getPersistentDataContainer().get(transitOwnerKey, PersistentDataType.STRING);
        if (id == null || owner == null) return;
        Location hit = pearl.getLocation();
        PearlImpact impact = PearlImpact.newBuilder()
            .setId(id).setOwnerUuid(owner).setWorld(hit.getWorld().getName())
            .setX(hit.getX()).setY(hit.getY()).setZ(hit.getZ()).build();
        log.info("Pearl {} landed at ({},{},{})", id, hit.getX(), hit.getY(), hit.getZ());
        pearl.remove();
        plugin.getServer().getAsyncScheduler().runNow(plugin, task -> {
            if (!peers.finishPearl(impact)) log.warn("Could not finish pearl {}", id);
        });
    }

    /** Called from the peer's gRPC thread; only the source shard has the pending flight. */
    public boolean acceptImpact(PearlImpact impact) {
        if (!enabled || !valid(impact.getX(), impact.getY(), impact.getZ()) || owns(impact.getX())) {
            return false;
        }
        UUID id;
        UUID ownerId;
        try {
            id = UUID.fromString(impact.getId());
            ownerId = UUID.fromString(impact.getOwnerUuid());
        } catch (IllegalArgumentException e) {
            return false;
        }
        Pending flight = pending.get(id);
        if (flight == null || !flight.owner().equals(ownerId) || !flight.world().equals(impact.getWorld())
            || !pending.remove(id, flight)) return false;

        plugin.getServer().getGlobalRegionScheduler().run(plugin, task -> {
            Player owner = Bukkit.getPlayer(ownerId);
            if (owner == null) return;
            owner.getScheduler().run(plugin, playerTask -> {
                if (owner.isOnline() && owner.getWorld().getName().equals(impact.getWorld())) {
                    BoundaryTransferListener handover = transfers;
                    if (handover != null) {
                        boolean started = handover.forceHandoverAt(owner,
                            impact.getX(), impact.getY(), impact.getZ(), true);
                        log.info("Pearl {} handover for {} started={}", id, owner.getName(), started);
                    }
                }
            }, () -> {});
        });
        return true;
    }

    private static boolean valid(double x, double y, double z) {
        return Double.isFinite(x) && Double.isFinite(y) && Double.isFinite(z)
            && Math.abs(x) < 30_000_000 && Math.abs(z) < 30_000_000 && y > -2048 && y < 2048;
    }
}
