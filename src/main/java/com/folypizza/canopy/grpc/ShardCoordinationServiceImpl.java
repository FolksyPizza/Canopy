package com.folypizza.canopy.grpc;

import com.folypizza.canopy.entity.EntityTracker;
import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.metrics.MetricsCollector;
import com.folypizza.canopy.model.TilePosition;
import com.folypizza.canopy.proto.ShardCoordinationServiceGrpc;
import com.folypizza.canopy.proto.ShardHealth;
import com.folypizza.canopy.proto.ShardHealthQuery;
import com.folypizza.canopy.proto.ShardInfo;
import com.folypizza.canopy.proto.ShardInfoQuery;
import com.folypizza.canopy.proto.TileVersionQuery;
import com.folypizza.canopy.proto.TileVersionResponse;
import com.folypizza.canopy.routing.PlayerRoutingProxy;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service exposing this shard's coordination surface to peers:
 * health telemetry, ownership lookups, and a tile-version stream.
 */
public class ShardCoordinationServiceImpl extends ShardCoordinationServiceGrpc.ShardCoordinationServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardCoordinationServiceImpl.class);

    private final long shardId;
    private final String host;
    private final MetricsCollector metrics;
    private final EntityTracker entityTracker;
    private final PlayerRoutingProxy routing;
    private final PartitionMap partitionMap;
    private final TileVersionServiceImpl tileVersionService;
    private final com.folypizza.canopy.routing.PlayerStateInbox playerStateInbox;
    private final com.folypizza.canopy.halo.HaloEditStore haloEditStore;
    private final java.util.function.LongSupplier worldTimeSupplier;

    public ShardCoordinationServiceImpl(long shardId, String host, MetricsCollector metrics,
                                        EntityTracker entityTracker, PlayerRoutingProxy routing,
                                        PartitionMap partitionMap, TileVersionServiceImpl tileVersionService,
                                        com.folypizza.canopy.routing.PlayerStateInbox playerStateInbox,
                                        com.folypizza.canopy.halo.HaloEditStore haloEditStore,
                                        java.util.function.LongSupplier worldTimeSupplier) {
        this.worldTimeSupplier = worldTimeSupplier;
        this.shardId = shardId;
        this.host = host;
        this.metrics = metrics;
        this.entityTracker = entityTracker;
        this.routing = routing;
        this.partitionMap = partitionMap;
        this.tileVersionService = tileVersionService;
        this.playerStateInbox = playerStateInbox;
        this.haloEditStore = haloEditStore;
    }

    @Override
    public void getShardInfo(ShardInfoQuery request, StreamObserver<ShardInfo> responseObserver) {
        TilePosition tile = new TilePosition(request.getTile().getX(), request.getTile().getZ());
        long owner = partitionMap.getState().findShardForTileOrUnowned(tile);
        ShardInfo info = ShardInfo.newBuilder()
            .setShardId(owner >= 0 ? owner : shardId)
            .setHost(host)
            .setRegionId("shard-" + shardId)
            .setTile(request.getTile())
            .build();
        responseObserver.onNext(info);
        responseObserver.onCompleted();
    }

    @Override
    public void getHealth(ShardHealthQuery request, StreamObserver<ShardHealth> responseObserver) {
        ShardHealth.Builder health = ShardHealth.newBuilder()
            .setShardId(shardId)
            .setTps(metrics.calcGlobalTPS())
            .setMspt(metrics.calcGlobalMsptP50())
            .setPlayerCount(routing.getActiveRoutes())
            .setEntityCount(entityTracker.getTrackedCount())
            .setFluidTicksInLastPhase(0)
            .setChunkIoPending(0)
            .setWorldTime(worldTimeSupplier != null ? worldTimeSupplier.getAsLong() : 0L);
        for (var st : entityTracker.getEntityStates().values()) {
            if (!"player".equals(st.entityType())) continue;
            health.addPlayers(com.folypizza.canopy.proto.PlayerPos.newBuilder()
                .setId(st.entityId().toString())
                .setX(st.x())
                .setZ(st.z())
                .build());
        }
        responseObserver.onNext(health.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTileVersionStream(TileVersionQuery request, StreamObserver<TileVersionResponse> responseObserver) {
        // Delegate to the unary tile-version logic; it emits one response then completes.
        tileVersionService.getUpdatedTiles(request, responseObserver);
    }

    @Override
    public void pushPlayerState(com.folypizza.canopy.proto.PlayerStatePush request,
                                StreamObserver<com.folypizza.canopy.proto.PlayerStateAck> responseObserver) {
        try {
            java.util.UUID id = java.util.UUID.fromString(request.getUuid());
            playerStateInbox.put(id, request.getBlob().toByteArray());
            log.info("Received player state for {} ({} bytes)", id, request.getBlob().size());
            responseObserver.onNext(com.folypizza.canopy.proto.PlayerStateAck.newBuilder().setOk(true).build());
        } catch (Exception e) {
            log.warn("pushPlayerState failed: {}", e.getMessage());
            responseObserver.onNext(com.folypizza.canopy.proto.PlayerStateAck.newBuilder().setOk(false).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getHaloEdits(com.folypizza.canopy.proto.HaloEditsQuery request,
                             StreamObserver<com.folypizza.canopy.proto.HaloEdits> responseObserver) {
        com.folypizza.canopy.proto.HaloEdits.Builder resp = com.folypizza.canopy.proto.HaloEdits.newBuilder();
        long maxSeq = request.getSinceSeq();
        for (var e : haloEditStore.since(request.getSinceSeq(), request.getMinX(), request.getMaxX())) {
            resp.addEdits(com.folypizza.canopy.proto.BlockEdit.newBuilder()
                .setX(e.x()).setY(e.y()).setZ(e.z()).setData(e.data()).setSeq(e.seq()).build());
            if (e.seq() > maxSeq) maxSeq = e.seq();
        }
        resp.setMaxSeq(maxSeq);
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }
}
