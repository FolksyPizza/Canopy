package com.folypizza.canopy.grpc;

import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.model.TilePosition;
import com.folypizza.canopy.proto.ShardCoordinationServiceGrpc;
import com.folypizza.canopy.proto.ShardHealth;
import com.folypizza.canopy.proto.ShardHealthQuery;
import com.folypizza.canopy.registry.ShardRegistry;
import com.folypizza.canopy.transfer.TileTransferService;
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
public class PeerManager {
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
                var ack = stub.pushPlayerState(com.folypizza.canopy.proto.PlayerStatePush.newBuilder()
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
            var resp = stub.getHaloEdits(com.folypizza.canopy.proto.HaloEditsQuery.newBuilder()
                .setMinX(minX).setMaxX(maxX).setSinceSeq(since).build());
            log.info("[halo] pull peer strip [{}..{}] since={} -> {} edits", minX, maxX, since, resp.getEditsCount());
            if (resp.getEditsCount() == 0) return;
            haloLastSeq.put(addr, resp.getMaxSeq());
            applyHaloEdits(resp.getEditsList());
            log.info("[halo] mirrored {} edits from peer strip [{}..{}]", resp.getEditsCount(), minX, maxX);
        } catch (Exception e) {
            log.warn("[halo] pull from {} failed: {}", addr, e.getMessage());
        }
    }

    private void applyHaloEdits(java.util.List<com.folypizza.canopy.proto.BlockEdit> edits) {
        if (plugin.getServer().getWorlds().isEmpty()) return;
        org.bukkit.World world = plugin.getServer().getWorlds().get(0);
        // Group by chunk to schedule one region task per chunk.
        java.util.Map<Long, java.util.List<com.folypizza.canopy.proto.BlockEdit>> byChunk = new java.util.HashMap<>();
        for (var e : edits) {
            long key = (((long) (e.getX() >> 4)) << 32) | ((e.getZ() >> 4) & 0xffffffffL);
            byChunk.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(e);
        }
        for (var entry : byChunk.entrySet()) {
            int cx = (int) (entry.getKey() >> 32);
            int cz = (int) (long) entry.getKey();
            var chunkEdits = entry.getValue();
            plugin.getServer().getRegionScheduler().run(plugin, world, cx, cz, task -> {
                for (var e : chunkEdits) {
                    try {
                        org.bukkit.block.data.BlockData bd = org.bukkit.Bukkit.createBlockData(e.getData());
                        world.getBlockAt(e.getX(), e.getY(), e.getZ()).setBlockData(bd, false);
                    } catch (Exception ex) {
                        // skip malformed block data
                    }
                }
            });
        }
    }

    public void start() {
        if (peerAddresses == null || peerAddresses.isEmpty()) {
            log.info("No peers configured (shard.peers empty) — running standalone");
            return;
        }
        running = true;
        for (String address : peerAddresses) {
            String addr = address.trim();
            if (addr.isEmpty()) continue;
            try {
                ManagedChannel channel = ManagedChannelBuilder.forTarget(addr)
                    .usePlaintext()
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

                log.info("[peer] {} shard={} tps={} mspt={} players={} entities={}",
                    addr, peerId, String.format("%.2f", health.getTps()),
                    String.format("%.2f", health.getMspt()), health.getPlayerCount(),
                    health.getEntityCount());

                // Mirror the peer's near-seam block edits into our local halo strip.
                pullHaloEdits(addr, channel);
            } catch (Exception e) {
                log.warn("[peer] health poll to {} failed: {}", addr, e.getMessage());
            }
        }
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
