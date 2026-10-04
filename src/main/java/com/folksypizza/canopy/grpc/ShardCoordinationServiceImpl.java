package com.folksypizza.canopy.grpc;

import com.folksypizza.canopy.entity.EntityTracker;
import com.folksypizza.canopy.leader.PartitionMap;
import com.folksypizza.canopy.metrics.MetricsCollector;
import com.folksypizza.canopy.model.TilePosition;
import com.folksypizza.canopy.proto.ShardCoordinationServiceGrpc;
import com.folksypizza.canopy.proto.ShardHealth;
import com.folksypizza.canopy.proto.ShardHealthQuery;
import com.folksypizza.canopy.proto.ShardInfo;
import com.folksypizza.canopy.proto.ShardInfoQuery;
import com.folksypizza.canopy.proto.TileVersionQuery;
import com.folksypizza.canopy.proto.TileVersionResponse;
import com.folksypizza.canopy.routing.PlayerRoutingProxy;
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
    private final com.folksypizza.canopy.routing.PlayerStateInbox playerStateInbox;
    private final com.folksypizza.canopy.routing.PearlTransitManager pearlTransitManager;
    private final com.folksypizza.canopy.halo.HaloEditStore haloEditStore;
    private final java.util.function.LongSupplier worldTimeSupplier;
    private final java.util.function.IntSupplier weatherSupplier;

    public ShardCoordinationServiceImpl(long shardId, String host, MetricsCollector metrics,
                                        EntityTracker entityTracker, PlayerRoutingProxy routing,
                                        PartitionMap partitionMap, TileVersionServiceImpl tileVersionService,
                                        com.folksypizza.canopy.routing.PlayerStateInbox playerStateInbox,
                                        com.folksypizza.canopy.halo.HaloEditStore haloEditStore,
                                        com.folksypizza.canopy.routing.PearlTransitManager pearlTransitManager,
                                        java.util.function.LongSupplier worldTimeSupplier,
                                        java.util.function.IntSupplier weatherSupplier) {
        this.worldTimeSupplier = worldTimeSupplier;
        this.weatherSupplier = weatherSupplier;
        this.shardId = shardId;
        this.host = host;
        this.metrics = metrics;
        this.entityTracker = entityTracker;
        this.routing = routing;
        this.partitionMap = partitionMap;
        this.tileVersionService = tileVersionService;
        this.playerStateInbox = playerStateInbox;
        this.haloEditStore = haloEditStore;
        this.pearlTransitManager = pearlTransitManager;
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
            .setWorldTime(worldTimeSupplier != null ? worldTimeSupplier.getAsLong() : 0L)
            .setWeatherBits(weatherSupplier != null ? weatherSupplier.getAsInt() : 0);
        for (var st : entityTracker.getEntityStates().values()) {
            if (!"player".equals(st.entityType())) continue;
            health.addPlayers(com.folksypizza.canopy.proto.PlayerPos.newBuilder()
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

    private volatile com.folksypizza.canopy.routing.ArrivalPrep arrivalPrep;

    public void setArrivalPrep(com.folksypizza.canopy.routing.ArrivalPrep arrivalPrep) {
        this.arrivalPrep = arrivalPrep;
    }

    @Override
    public void prepareArrival(com.folksypizza.canopy.proto.ArrivalHint request,
                               StreamObserver<com.folksypizza.canopy.proto.PlayerStateAck> responseObserver) {
        boolean ok = false;
        try {
            var prep = arrivalPrep;
            if (prep != null) {
                prep.hint(java.util.UUID.fromString(request.getUuid()), request.getWorld(), request.getX(), request.getZ());
                ok = true;
            }
        } catch (Exception e) {
            log.warn("prepareArrival failed: {}", e.getMessage());
        }
        responseObserver.onNext(com.folksypizza.canopy.proto.PlayerStateAck.newBuilder().setOk(ok).build());
        responseObserver.onCompleted();
    }

    @Override
    public void pushPlayerState(com.folksypizza.canopy.proto.PlayerStatePush request,
                                StreamObserver<com.folksypizza.canopy.proto.PlayerStateAck> responseObserver) {
        try {
            java.util.UUID id = java.util.UUID.fromString(request.getUuid());
            boolean accepted = request.getBlob().size() <= com.folksypizza.canopy.routing.PlayerStateInbox.MAX_BLOB_BYTES
                && playerStateInbox.put(id, request.getBlob().toByteArray());
            log.info("Received player state for {} ({} bytes){}", id, request.getBlob().size(), accepted ? "" : ", refused");
            responseObserver.onNext(com.folksypizza.canopy.proto.PlayerStateAck.newBuilder().setOk(accepted).build());
        } catch (Exception e) {
            log.warn("pushPlayerState failed: {}", e.getMessage());
            responseObserver.onNext(com.folksypizza.canopy.proto.PlayerStateAck.newBuilder().setOk(false).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void relayPearl(com.folksypizza.canopy.proto.PearlFlight request,
                           StreamObserver<com.folksypizza.canopy.proto.PearlAck> responseObserver) {
        responseObserver.onNext(com.folksypizza.canopy.proto.PearlAck.newBuilder()
            .setOk(pearlTransitManager.acceptFlight(request)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void finishPearl(com.folksypizza.canopy.proto.PearlImpact request,
                            StreamObserver<com.folksypizza.canopy.proto.PearlAck> responseObserver) {
        responseObserver.onNext(com.folksypizza.canopy.proto.PearlAck.newBuilder()
            .setOk(pearlTransitManager.acceptImpact(request)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getHaloEdits(com.folksypizza.canopy.proto.HaloEditsQuery request,
                             StreamObserver<com.folksypizza.canopy.proto.HaloEdits> responseObserver) {
        com.folksypizza.canopy.proto.HaloEdits.Builder resp = com.folksypizza.canopy.proto.HaloEdits.newBuilder();
        long maxSeq = request.getSinceSeq();
        for (var e : haloEditStore.since(request.getSinceSeq(), request.getMinX(), request.getMaxX())) {
            resp.addEdits(com.folksypizza.canopy.proto.BlockEdit.newBuilder()
                .setX(e.x()).setY(e.y()).setZ(e.z()).setData(e.data()).setSeq(e.seq()).build());
            if (e.seq() > maxSeq) maxSeq = e.seq();
        }
        resp.setMaxSeq(maxSeq);
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }
}
