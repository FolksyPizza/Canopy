package com.folypizza.canopy.grpc;

import com.folypizza.canopy.halo.TileUpdateTracker;
import com.folypizza.canopy.halo.TileVersionStore;
import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.model.TilePosition;
import com.folypizza.canopy.proto.TileVersion;
import com.folypizza.canopy.proto.TileVersionQuery;
import com.folypizza.canopy.proto.TileVersionResponse;
import com.folypizza.canopy.proto.TileVersionServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * gRPC service implementation for tile version queries.
 *
 * Neighboring shards query this service to find which tiles have been
 * updated since they last pulled. Returns only tiles with a higher
 * version than the query's known version.
 */
public class TileVersionServiceImpl extends TileVersionServiceGrpc.TileVersionServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(TileVersionServiceImpl.class);

    private final long shardId;
    private final TileVersionStore tileVersionStore;
    private final TileUpdateTracker updateTracker;
    private final PartitionMap partitionMap;

    public TileVersionServiceImpl(
        long shardId,
        TileVersionStore tileVersionStore,
        TileUpdateTracker updateTracker,
        PartitionMap partitionMap
    ) {
        this.shardId = shardId;
        this.tileVersionStore = tileVersionStore;
        this.updateTracker = updateTracker;
        this.partitionMap = partitionMap;
    }

    @Override
    public void getUpdatedTiles(TileVersionQuery query, StreamObserver<TileVersionResponse> responseObserver) {
        // Highest version the querying shard already holds. Anything newer is a delta.
        int knownMax = query.getKnownVersionsList().stream()
            .mapToInt(TileVersion::getVersion)
            .max()
            .orElse(0);

        boolean hasBounds = query.hasMinTile() && query.hasMaxTile();
        com.folypizza.canopy.proto.TilePosition minTile = query.getMinTile();
        com.folypizza.canopy.proto.TilePosition maxTile = query.getMaxTile();

        TileVersionResponse.Builder response = TileVersionResponse.newBuilder()
            .setShardId(shardId)
            .setCurrentVersion(tileVersionStore.getMaxVersion());

        int included = 0;
        for (var entry : tileVersionStore.getAllLatestVersions().entrySet()) {
            int version = entry.getValue();
            if (version <= knownMax) continue;

            TilePosition tile = TileVersionStore.keyToTile(entry.getKey());
            if (hasBounds && (tile.x() < minTile.getX() || tile.x() > maxTile.getX()
                    || tile.z() < minTile.getZ() || tile.z() > maxTile.getZ())) {
                continue;
            }

            response.addTiles(com.folypizza.canopy.proto.TileData.newBuilder()
                .setPosition(com.folypizza.canopy.proto.TilePosition.newBuilder()
                    .setX(tile.x())
                    .setZ(tile.z()))
                .setVersion(version)
                .build());
            included++;
        }

        log.debug("TileVersionQuery from shard {} (knownMax={}) -> {} updated tiles",
            query.getOriginShardId(), knownMax, included);

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    private TilePosition toTilePosition(com.folypizza.canopy.proto.TilePosition proto) {
        return new TilePosition(proto.getX(), proto.getZ());
    }

    /**
     * Get the tile version for a specific tile position (for halo sync).
     */
    public int getTileVersion(TilePosition tile) {
        return tileVersionStore.getVersion(tile);
    }

    /**
     * Record a tile update (called when block state changes).
     * This method should be called by the world module whenever a block
     * in a monitored tile is modified.
     */
    public void recordUpdate(TilePosition tile) {
        tileVersionStore.incrementVersion(tile);
        updateTracker.recordUpdate(tile);
    }

    /**
     * Get a snapshot of all tile versions (for initial halo population).
     */
    public Map<TilePosition, Integer> getAllVersions() {
        return tileVersionStore.getAllLatestVersions().entrySet().stream()
            .collect(Collectors.toMap(
                e -> TileVersionStore.keyToTile(((Map.Entry<Long, Integer>) e).getKey()),
                e -> ((Map.Entry<Long, Integer>) e).getValue()
            ));
    }
}
