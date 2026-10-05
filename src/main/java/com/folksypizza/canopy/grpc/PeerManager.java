package com.folksypizza.canopy.grpc;

import com.folksypizza.canopy.leader.PartitionMap;
import com.folksypizza.canopy.model.TilePosition;
import com.folksypizza.canopy.proto.ShardCoordinationServiceGrpc;
import com.folksypizza.canopy.proto.ShardHealth;
import com.folksypizza.canopy.proto.ShardHealthQuery;
import com.folksypizza.canopy.registry.ShardRegistry;
import com.folksypizza.canopy.transfer.TileTransferService;
import com.folksypizza.canopy.halo.PendingHaloEdits;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Connects this shard to its configured peers and keeps the link live.
 *
 * For each peer address it opens a gRPC channel, resolves the peer's shard id via
 * the coordination service, registers it with the {@link ShardRegistry} and the
 * {@link TileTransferService} (so halo pulls can reach it), and polls peer health
 * on a fixed cadence. This is what turns a set of independent shards into a
 * coordinating cluster.
 */
public class PeerManager implements org.bukkit.event.Listener {
    private static final Logger log = LoggerFactory.getLogger(PeerManager.class);
    private static final long POLL_INTERVAL_SEC = 5;
    private static final long RPC_DEADLINE_SEC = 3;

    private final long localShardId;
    private final List<String> peerAddresses;
    private final TileTransferService tileTransferService;
    private final ShardRegistry shardRegistry;
    private final PartitionMap partitionMap;
    private final org.bukkit.plugin.java.JavaPlugin plugin;
    private final double boundaryX;
    private final int haloWidth;
    private final boolean ownsWest;
    // Per-peer high-water seq for halo edits already applied.
    private final ConcurrentHashMap<String, Long> haloLastSeq = new ConcurrentHashMap<>();
    private final PendingHaloEdits pendingHaloEdits = new PendingHaloEdits(16_384);
    // Latest world time reported by each peer (peer shard id -> full time), for time sync.
    private final ConcurrentHashMap<Long, Long> peerWorldTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> peerWeather = new ConcurrentHashMap<>();
    private static final long PEER_TIMEOUT_MS = 15_000;
    private volatile long lastPeerContactMs = 0;
    // Set by each poll: true after a successful health RPC, false after a failed one, so a
    // downed peer is detected within one poll interval rather than only after the timeout.
    private volatile boolean peerReachable = false;

    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TileVersionClient> tileClients = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> resolvedIds = new ConcurrentHashMap<>();
    // Last player list reported by each peer (peer shard id -> [{id,x,z,shard}]).
    private final ConcurrentHashMap<Long, java.util.List<java.util.Map<String, Object>>> peerPlayers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "canopy-peer-manager");
            t.setDaemon(true);
            return t;
        });
    private volatile boolean running = false;

    public PeerManager(long localShardId, List<String> peerAddresses,
                       TileTransferService tileTransferService, ShardRegistry shardRegistry,
                       PartitionMap partitionMap, org.bukkit.plugin.java.JavaPlugin plugin,
                       double boundaryX, int haloWidth, boolean ownsWest) {
        this.localShardId = localShardId;
        this.peerAddresses = peerAddresses;
        this.tileTransferService = tileTransferService;
        this.shardRegistry = shardRegistry;
        this.partitionMap = partitionMap;
        this.plugin = plugin;
        this.boundaryX = boundaryX;
        this.haloWidth = haloWidth;
        this.ownsWest = ownsWest;
    }

    /**
     * Push a player's state blob to the (single) peer shard so it can apply it on join.
     * Returns true if the peer acknowledged.
     */
    public boolean pushPlayerState(String uuid, byte[] blob) {
        for (ManagedChannel channel : channels.values()) {
            try {
                var stub = ShardCoordinationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SEC, TimeUnit.SECONDS);
                var ack = stub.pushPlayerState(com.folksypizza.canopy.proto.PlayerStatePush.newBuilder()
                    .setUuid(uuid)
                    .setBlob(com.google.protobuf.ByteString.copyFrom(blob))
                    .build());
                if (ack.getOk()) return true;
            } catch (Exception e) {
                log.warn("pushPlayerState to peer failed: {}", e.getMessage());
            }
        }
        return false;
    }

    /** Ghost phase: tell the peer where an approaching player is predicted to land. Best effort. */
    public void prepareArrival(com.folksypizza.canopy.proto.ArrivalHint hint) {
        for (ManagedChannel channel : channels.values()) {
            try {
                ShardCoordinationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SEC, TimeUnit.SECONDS)
                    .prepareArrival(hint);
            } catch (Exception e) {
                log.debug("prepareArrival to peer failed: {}", e.getMessage());
            }
        }
    }

    public boolean relayPearl(com.folksypizza.canopy.proto.PearlFlight flight) {
        for (ManagedChannel channel : channels.values()) {
            try {
                var ack = ShardCoordinationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SEC, TimeUnit.SECONDS)
                    .relayPearl(flight);
                if (ack.getOk()) return true;
            } catch (Exception e) {
                log.warn("relayPearl to peer failed: {}", e.getMessage());
            }
        }
        return false;
    }

    public boolean finishPearl(com.folksypizza.canopy.proto.PearlImpact impact) {
        for (ManagedChannel channel : channels.values()) {
            try {
                var ack = ShardCoordinationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SEC, TimeUnit.SECONDS)
                    .finishPearl(impact);
                if (ack.getOk()) return true;
            } catch (Exception e) {
                log.warn("finishPearl to peer failed: {}", e.getMessage());
            }
        }
        return false;
    }

    /** Pull the peer's near-boundary block edits and mirror them into our local halo strip. */
    private void pullHaloEdits(String addr, ManagedChannel channel) {
        if (haloWidth <= 0 || plugin == null) return;
        // Request the PEER's owned strip (the opposite side of the boundary from us).
        int minX = ownsWest ? (int) boundaryX : (int) (boundaryX - haloWidth);
        int maxX = ownsWest ? (int) (boundaryX + haloWidth) : (int) boundaryX;
        long since = haloLastSeq.getOrDefault(addr, 0L);
        try {
            var stub = ShardCoordinationServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(RPC_DEADLINE_SEC, TimeUnit.SECONDS);
            var resp = stub.getHaloEdits(com.folksypizza.canopy.proto.HaloEditsQuery.newBuilder()
                .setMinX(minX).setMaxX(maxX).setSinceSeq(since).build());
            if (resp.getEditsCount() == 0) return;
            if (!applyHaloEdits(resp.getEditsList())) {
                // Keep the sequence cursor unchanged so the source can retry after pending chunks load.
                log.warn("[halo] pending edit buffer is full; retaining cursor for {} edits from {}",
                    resp.getEditsCount(), addr);
                return;
            }
            haloLastSeq.put(addr, resp.getMaxSeq());
            log.debug("[halo] queued {} edits from peer strip [{}..{}]", resp.getEditsCount(), minX, maxX);
        } catch (Exception e) {
            log.warn("[halo] pull from {} failed: {}", addr, e.getMessage());
        }
    }

    private boolean applyHaloEdits(java.util.List<com.folksypizza.canopy.proto.BlockEdit> edits) {
        if (plugin.getServer().getWorlds().isEmpty()) return false;
        org.bukkit.World world = plugin.getServer().getWorlds().get(0);
        java.util.Set<Long> touchedChunks = new java.util.HashSet<>();
        boolean accepted = true;
        for (var e : edits) {
            if (!pendingHaloEdits.offer(new PendingHaloEdits.Edit(
                e.getSeq(), e.getX(), e.getY(), e.getZ(), e.getData()))) {
                accepted = false;
                break;
            }
            touchedChunks.add(PendingHaloEdits.chunkKey(e.getX() >> 4, e.getZ() >> 4));
        }
        for (long key : touchedChunks) {
            scheduleHaloApply(world, (int) (key >> 32), (int) key);
        }
        return accepted;
    }

    private void scheduleHaloApply(org.bukkit.World world, int chunkX, int chunkZ) {
        plugin.getServer().getRegionScheduler().run(plugin, world, chunkX, chunkZ,
            task -> applyPendingHaloEdits(world, chunkX, chunkZ));
    }

    private void applyPendingHaloEdits(org.bukkit.World world, int chunkX, int chunkZ) {
        // RegionScheduler guarantees the owner thread, but the chunk may still be generating or
        // may have unloaded before this task ran. Leave the edits queued for its next load event.
        if (!world.isChunkLoaded(chunkX, chunkZ)) return;
        var edits = pendingHaloEdits.drain(chunkX, chunkZ);
        if (edits.isEmpty()) return;
        org.bukkit.Chunk chunk = world.getChunkAt(chunkX, chunkZ);
        for (var edit : edits) {
            try {
                org.bukkit.block.data.BlockData data = org.bukkit.Bukkit.createBlockData(edit.data());
                chunk.getBlock(edit.x() & 15, edit.y(), edit.z() & 15).setBlockData(data, false);
            } catch (Exception ex) {
                log.debug("Skipping invalid halo edit at {},{},{}: {}",
                    edit.x(), edit.y(), edit.z(), ex.getMessage());
            }
        }
    }

    @org.bukkit.event.EventHandler(priority = org.bukkit.event.EventPriority.MONITOR)
    public void onChunkLoad(org.bukkit.event.world.ChunkLoadEvent event) {
        int chunkX = event.getChunk().getX();
        int chunkZ = event.getChunk().getZ();
        if (pendingHaloEdits.hasChunk(chunkX, chunkZ)) {
            scheduleHaloApply(event.getWorld(), chunkX, chunkZ);
        }
    }

    public void start() {
        if (peerAddresses == null || peerAddresses.isEmpty()) {
            log.info("No peers configured (shard.peers empty) — running standalone");
            return;
        }
        running = true;
        plugin.getServer().getPluginManager().registerEvents(this, plugin);
        for (String address : peerAddresses) {
            String addr = address.trim();
            if (addr.isEmpty()) continue;
            try {
                ManagedChannel channel = ManagedChannelBuilder.forTarget(addr)
                    .usePlaintext()
                    .intercept(SharedSecret.current().client())
                    .build();
                channels.put(addr, channel);
                TileVersionClient tvc = new TileVersionClient(addr);
                tileClients.put(addr, tvc);
                log.info("Peer channel opened to {}", addr);
            } catch (Exception e) {
                log.warn("Failed to open peer channel to {}: {}", addr, e.getMessage());
            }
        }
        executor.scheduleAtFixedRate(this::pollPeers, 2, POLL_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("PeerManager started ({} peers)", channels.size());
    }

    private void pollPeers() {
        if (!running) return;
        for (var entry : channels.entrySet()) {
            String addr = entry.getKey();
            ManagedChannel channel = entry.getValue();
            try {
                var stub = ShardCoordinationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SEC, TimeUnit.SECONDS);
                ShardHealth health = stub.getHealth(ShardHealthQuery.newBuilder()
                    .setShardId(localShardId)
                    .build());

                long peerId = health.getShardId();
                Long known = resolvedIds.put(addr, peerId);

                // Register / refresh the peer in the shard registry.
                if (shardRegistry.getShard(peerId) == null) {
                    shardRegistry.register(new ShardRegistry.ShardEntry(peerId, addr, addr, 1));
                }
                shardRegistry.heartbeat(peerId, health.getTps(), health.getMspt(), health.getPlayerCount());

                // First time we learn the peer's real id, wire it for halo pulls.
                if (known == null || known != peerId) {
                    TileVersionClient tvc = tileClients.get(addr);
                    if (tvc != null) {
                        tileTransferService.addPeer(peerId, tvc);
                    }
                }

                // Capture the peer's live player positions for the unified dashboard.
                java.util.List<java.util.Map<String, Object>> plist = new java.util.ArrayList<>();
                for (var pp : health.getPlayersList()) {
                    java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
                    m.put("id", pp.getId());
                    m.put("x", Math.round(pp.getX() * 100.0) / 100.0);
                    m.put("z", Math.round(pp.getZ() * 100.0) / 100.0);
                    m.put("shard", peerId);
                    plist.add(m);
                }
                peerPlayers.put(peerId, plist);
                peerWorldTimes.put(peerId, health.getWorldTime());
                peerWeather.put(peerId, health.getWeatherBits());
                lastPeerContactMs = System.currentTimeMillis();
                peerReachable = true;
                peerMspt = health.getMspt();

                log.debug("[peer] {} shard={} tps={} mspt={} players={}",
                    addr, peerId, String.format("%.2f", health.getTps()),
                    String.format("%.2f", health.getMspt()), health.getPlayerCount());

                // Mirror the peer's near-seam block edits into our local halo strip.
                pullHaloEdits(addr, channel);
            } catch (Exception e) {
                peerReachable = false;
                log.warn("[peer] health poll to {} failed: {}", addr, e.getMessage());
            }
        }
    }

    /**
     * True if the destination peer is reachable: the most recent poll succeeded and it was
     * recent. A downed peer flips this false on the next poll (~one poll interval).
     */
    private volatile double peerMspt;

    /** The peer's tick time from its last health response (0 until known). */
    public double getPeerMspt() {
        return peerMspt;
    }

    public boolean isPeerHealthy() {
        return peerReachable && (System.currentTimeMillis() - lastPeerContactMs < PEER_TIMEOUT_MS);
    }

    /**
     * World time of the time authority (the reachable peer with the lowest shard id, when
     * that id is below ours), or -1 if this shard is the authority / no peer known.
     */
    public long getAuthorityWorldTime(long localShardId) {
        long bestId = Long.MAX_VALUE, bestTime = -1;
        for (var e : peerWorldTimes.entrySet()) {
            if (e.getKey() < localShardId && e.getKey() < bestId) {
                bestId = e.getKey();
                bestTime = e.getValue();
            }
        }
        return bestTime;
    }

    /**
     * Weather bitfield (bit0 = storm/rain, bit1 = thundering) of the authority — the reachable
     * peer with the lowest shard id, when that id is below ours — or -1 if this shard is the
     * authority / no peer is known. Mirrors {@link #getAuthorityWorldTime(long)}.
     */
    public int getAuthorityWeatherBits(long localShardId) {
        long bestId = Long.MAX_VALUE;
        int bestWeather = -1;
        for (var e : peerWeather.entrySet()) {
            if (e.getKey() < localShardId && e.getKey() < bestId) {
                bestId = e.getKey();
                bestWeather = e.getValue();
            }
        }
        return bestWeather;
    }

    public int getConnectedPeerCount() {
        return resolvedIds.size();
    }

    /** Players reported by all peers (for the cluster-wide map). */
    public java.util.List<java.util.Map<String, Object>> getPeerPlayers() {
        java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
        for (var list : peerPlayers.values()) out.addAll(list);
        return out;
    }

    public void stop() {
        running = false;
        executor.shutdownNow();
        for (TileVersionClient c : tileClients.values()) {
            try { c.close(); } catch (Exception ignored) { }
        }
        for (ManagedChannel ch : channels.values()) {
            try { ch.shutdownNow(); } catch (Exception ignored) { }
        }
        channels.clear();
        tileClients.clear();
    }
}
