package com.folypizza.canopy.transfer;

import com.folypizza.canopy.grpc.TileVersionClient;
import com.folypizza.canopy.grpc.TileVersionServiceImpl;
import com.folypizza.canopy.halo.TileUpdateTracker;
import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.model.TilePosition;
import com.folypizza.canopy.proto.TileVersionQuery;
import com.folypizza.canopy.proto.TileVersionResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tile transfer service — Phase 1 seam delta streaming.
 *
 * When a player approaches a seam, requests halo tile data from
 * neighboring shards via gRPC and caches it locally for rendering.
 * Uses lazy-pull invalidation: only pulls tiles the local shard
 * does not know about, based on tile version comparisons.
 *
 * Batches tile updates into windows (default 10ms) and coalesces
 * requests for adjacent tiles to minimize RPC overhead.
 */
public class TileTransferService {
    private static final Logger log = LoggerFactory.getLogger(TileTransferService.class);
    private static final int TILE_VERSION_WINDOW_MS = 10;
    private static final int MAX_BATCH_TILES = 64;

    private final TileVersionServiceImpl localTileService;
    private final ConcurrentHashMap<String, TileVersionClient> peerClients = new ConcurrentHashMap<>();
    private final PartitionMap partitionMap;
    private final TileUpdateTracker tileTracker;
    private final ScheduledExecutorService executor;
    private final ConcurrentHashMap<TilePosition, TileDataEntry> localCache = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    public record TileDataEntry(
        com.folypizza.canopy.proto.TileVersion protoVersion,
        long currentVersion,
        long receivedMs
    ) {}

    public TileTransferService(
        TileVersionServiceImpl localTileService,
        PartitionMap partitionMap,
        TileUpdateTracker tileTracker
    ) {
        this.localTileService = localTileService;
        this.partitionMap = partitionMap;
        this.tileTracker = tileTracker;
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "canopy-tile-transfer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Resolve the local shard id from the current partition state.
     */
    private long localShardId() {
        var shards = partitionMap.getState().shards();
        return shards.isEmpty() ? 0L : shards.get(0).shardId();
    }

    /**
     * Invoke a gRPC unary call and block for the single response.
     */
    private TileVersionResponse collectResponse(java.util.function.Consumer<StreamObserver<TileVersionResponse>> caller) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TileVersionResponse> ref = new AtomicReference<>();
        caller.accept(new StreamObserver<TileVersionResponse>() {
            @Override public void onNext(TileVersionResponse value) { ref.set(value); }
            @Override public void onError(Throwable t) { latch.countDown(); }
            @Override public void onCompleted() { latch.countDown(); }
        });
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TileVersionResponse r = ref.get();
        return r != null ? r : TileVersionResponse.getDefaultInstance();
    }

    /**
     * Register a neighboring shard's TileVersionService for tile data pull.
     */
    public void addPeer(long peerShardId, TileVersionClient client) {
        peerClients.put(String.valueOf(peerShardId), client);
        log.info("Registered peer shard {} for tile data", peerShardId);
    }

    /**
     * Pull halo tile data for tiles that belong to neighboring shards.
     * Called when a player approaches a seam boundary.
     *
     * @return list of tiles with fresh data from neighbors
     */
    public CompletableFuture<List<TileDataEntry>> pullHaloTiles(TilePosition centerTile, int radiusTiles) {
        return CompletableFuture.supplyAsync(() -> {
            List<TileDataEntry> result = new ArrayList<>();
            long minTileX = centerTile.x() - radiusTiles;
            long minTileZ = centerTile.z() - radiusTiles;
            long maxTileX = centerTile.x() + radiusTiles;
            long maxTileZ = centerTile.z() + radiusTiles;
            long localShardId = localShardId();

            for (var neighborId : peerClients.keySet()) {
                long peerId = Long.parseLong(neighborId);
                if (peerId == localShardId) continue;

                TileVersionClient client = peerClients.get(neighborId);
                if (client == null) continue;

                try {
                    TileVersionQuery query = TileVersionQuery.newBuilder()
                        .setOriginShardId(localShardId)
                        .setMinTile(
                            com.folypizza.canopy.proto.TilePosition.newBuilder()
                                .setX((int) minTileX)
                                .setZ((int) minTileZ)
                                .build()
                        )
                        .setMaxTile(
                            com.folypizza.canopy.proto.TilePosition.newBuilder()
                                .setX((int) maxTileX)
                                .setZ((int) maxTileZ)
                                .build()
                        )
                        .build();

                    TileVersionResponse response =
                        collectResponse(obs -> client.getUpdatedTilesAsync(query, obs));

                    for (var tile : response.getTilesList()) {
                        TilePosition tilePos = new TilePosition(tile.getPosition().getX(), tile.getPosition().getZ());
                        // Skip tiles owned by us
                        if (partitionMap.getState().findShardForTile(tilePos) == localShardId) continue;

                        long currentVersion = tile.getVersion();

                        // Check if we already have fresher data
                        TileDataEntry existing = localCache.get(tilePos);
                        if (existing != null && existing.currentVersion() >= currentVersion) continue;

                        var tv = com.folypizza.canopy.proto.TileVersion.newBuilder()
                            .setTile(tile.getPosition())
                            .setVersion((int) currentVersion)
                            .build();
                        
                        TileDataEntry entry = new TileDataEntry(tv, currentVersion, System.currentTimeMillis());
                        localCache.put(tilePos, entry);
                        result.add(entry);

                        log.debug("Pulled halo tile {} from shard {} v{}", tilePos, peerId, currentVersion);
                    }
                } catch (StatusRuntimeException e) {
                    log.warn("Failed to pull halo tiles from shard {}: {}", peerId, e.getStatus().getDescription());
                }
            }

            return result;
        }, executor);
    }

    /**
     * Batch-push local tile updates to all peers within the 10ms window.
     */
    public void pushLocalUpdates() {
        List<Map.Entry<Long, com.folypizza.canopy.proto.TileVersion>> entries = new ArrayList<>();

        for (var neighborId : peerClients.keySet()) {
            long peerId = Long.parseLong(neighborId);
            if (peerId == localShardId()) continue;

            // Query local shard for tiles changed since last push
            try {
                TileVersionQuery query = TileVersionQuery.newBuilder()
                    .setOriginShardId(localShardId())
                    .build();

                TileVersionResponse response =
                    collectResponse(obs -> localTileService.getUpdatedTiles(query, obs));

                for (var tile : response.getTilesList()) {
                    entries.add(Map.entry(peerId,
                        com.folypizza.canopy.proto.TileVersion.newBuilder()
                            .setTile(tile.getPosition())
                            .setVersion(tile.getVersion())
                            .build()));
                }
            } catch (Exception e) {
                log.warn("Failed to gather local tile updates for peer {}: {}", peerId, e.getMessage());
            }
        }

        if (!entries.isEmpty()) {
            // Batch and push via gRPC in next cycle
            tileTracker.markUpdatedTiles(entries);
        }
    }

    /**
     * Get cached halo tile data for a specific tile position.
     */
    public TileDataEntry getCachedTile(TilePosition tilePos) {
        return localCache.get(tilePos);
    }

    public int getCacheSize() {
        return localCache.size();
    }

    public int getPeerCount() {
        return peerClients.size();
    }

    /**
     * Clear cached halo tiles older than the stale TTL.
     */
    public void staleCacheCleanup(long staleTtlMs) {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<TilePosition, TileDataEntry>> it = localCache.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (now - entry.getValue().receivedMs() > staleTtlMs) {
                it.remove();
            }
        }
    }

    public void start() {
        running = true;
        executor.scheduleAtFixedRate(this::pushLocalUpdates,
            TILE_VERSION_WINDOW_MS, TILE_VERSION_WINDOW_MS, TimeUnit.MILLISECONDS);
        log.info("TileTransferService started (batch={}:{}ms)", TILE_VERSION_WINDOW_MS * 10, 10);
    }

    public void stop() {
        running = false;
        executor.shutdown();
    }

    public boolean isRunning() {
        return running;
    }
}
