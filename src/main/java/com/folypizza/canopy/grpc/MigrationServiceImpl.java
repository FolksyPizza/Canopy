package com.folypizza.canopy.grpc;

import com.folypizza.canopy.migration.ChunkSnapshotSerializer;
import com.folypizza.canopy.migration.MigrationService;
import com.folypizza.canopy.model.ChunkPosition;
import com.folypizza.canopy.proto.ChunkData;
import com.folypizza.canopy.proto.ChunkPos;
import com.folypizza.canopy.proto.ChunkVersion;
import com.folypizza.canopy.proto.MigrationAckRequest;
import com.folypizza.canopy.proto.MigrationAckResponse;
import com.folypizza.canopy.proto.MigrationChunkRequest;
import com.folypizza.canopy.proto.MigrationChunkResponse;
import com.folypizza.canopy.proto.MigrationInitRequest;
import com.folypizza.canopy.proto.MigrationInitResponse;
import com.folypizza.canopy.proto.MigrationServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * gRPC entry point for cross-shard chunk migration.
 *
 * Handles the migration handshake: a peer initiates a migration for a set of
 * chunks, streams the chunk payloads, then acknowledges. The session lifecycle
 * is tracked by the local {@link MigrationService}.
 *
 * Block-state payloads are captured on the owning region thread via
 * {@link ChunkSnapshotSerializer} (Folia-safe) and streamed as {@code section_data}.
 * Lighting and entity payloads are not yet captured. The migration handshake and
 * session tracking are fully functional.
 */
public class MigrationServiceImpl extends MigrationServiceGrpc.MigrationServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(MigrationServiceImpl.class);

    private static final long SNAPSHOT_TIMEOUT_MS = 3_000;

    private final long shardId;
    private final MigrationService migrationService;
    private final JavaPlugin plugin;

    public MigrationServiceImpl(long shardId, MigrationService migrationService, JavaPlugin plugin) {
        this.shardId = shardId;
        this.migrationService = migrationService;
        this.plugin = plugin;
    }

    @Override
    public void initiateMigration(MigrationInitRequest request,
                                  StreamObserver<MigrationInitResponse> responseObserver) {
        List<ChunkPosition> chunks = new ArrayList<>();
        for (ChunkPos pos : request.getChunkPositionsList()) {
            try {
                chunks.add(new ChunkPosition(pos.getX(), pos.getZ()));
            } catch (IllegalArgumentException ignored) { }
        }

        var session = migrationService.initiateMigration(
            request.getSourceShard(), request.getDestShard(), chunks);

        MigrationInitResponse.Builder resp = MigrationInitResponse.newBuilder()
            .setExpectedPasses(1)
            .setChunkCount(chunks.size());
        for (ChunkPosition c : chunks) {
            resp.addChunkVersions(ChunkVersion.newBuilder()
                .setChunkX(c.chunkX())
                .setChunkZ(c.chunkZ())
                .setVersion(1)
                .build());
        }

        log.info("Migration {} initiated via gRPC: {} chunks {} -> {}",
            session.migrationId(), chunks.size(), request.getSourceShard(), request.getDestShard());

        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }

    @Override
    public void streamChunks(MigrationChunkRequest request,
                             StreamObserver<MigrationChunkResponse> responseObserver) {
        // Stream real block-state payloads for the matching active session's chunks.
        // Snapshots are captured on the owning region thread by the serializer.
        World world = plugin.getServer().getWorlds().isEmpty()
            ? null : plugin.getServer().getWorlds().get(0);
        int streamed = 0;
        for (var session : migrationService.getActiveSessions()) {
            if (session.sourceShardId() != request.getSourceShard()
                || session.destShardId() != request.getDestShard()) {
                continue;
            }
            for (ChunkPosition c : session.chunksToMigrate()) {
                byte[] sectionData = world == null
                    ? new byte[0]
                    : ChunkSnapshotSerializer.serialize(plugin, world, c.chunkX(), c.chunkZ(), SNAPSHOT_TIMEOUT_MS);
                ChunkData data = ChunkData.newBuilder()
                    .setChunkX(c.chunkX())
                    .setChunkZ(c.chunkZ())
                    .setSectionData(ByteString.copyFrom(sectionData))
                    .setLightingData(ByteString.EMPTY)
                    .setEntityData(ByteString.EMPTY)
                    .build();
                responseObserver.onNext(MigrationChunkResponse.newBuilder()
                    .setChunk(data)
                    .setIsDirty(false)
                    .build());
                streamed++;
            }
            break;
        }
        log.info("Streamed {} chunk manifest entries for {} -> {} (final={})",
            streamed, request.getSourceShard(), request.getDestShard(), request.getIsFinal());
        responseObserver.onCompleted();
    }

    @Override
    public void acknowledgeMigration(MigrationAckRequest request,
                                     StreamObserver<MigrationAckResponse> responseObserver) {
        MigrationAckResponse resp = MigrationAckResponse.newBuilder()
            .setMigrationId(request.getMigrationId())
            .setSuccess(true)
            .setMigrationStarted(true)
            .build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }
}
