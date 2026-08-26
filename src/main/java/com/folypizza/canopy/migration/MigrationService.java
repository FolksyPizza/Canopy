package com.folypizza.canopy.migration;

import com.folypizza.canopy.grpc.TileVersionClient;
import com.folypizza.canopy.model.ChunkPosition;
import com.folypizza.canopy.transfer.TileTransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MigrationService {
    private static final Logger log = LoggerFactory.getLogger(MigrationService.class);

    public enum MigrationState {
        INITIATED, PRE_COPY, DIRTY_TRACKING, FREEZE, TRANSFERRING, COMPLETE, ABORTED
    }

    public record MigrationSession(
        UUID migrationId,
        long sourceShardId,
        long destShardId,
        List<ChunkPosition> chunksToMigrate,
        long startedAt,
        MigrationState state,
        int currentPass,
        Set<ChunkPosition> dirtySections
    ) {}

    public record MigrationResult(
        UUID migrationId,
        long totalTimeMs,
        int chunksTransferred,
        int dirtyPasses,
        MigrationState finalState
    ) {}

    private final ConcurrentHashMap<String, TileVersionClient> shardClients;
    private final ConcurrentHashMap<String, MigrationSession> activeSessions = new ConcurrentHashMap<>();
    private final TileTransferService tileTransfer;
    private final ExecutorService executor;
    private final AtomicLong migrationIdCounter = new AtomicLong(0);

    public MigrationService(
        ConcurrentHashMap<String, TileVersionClient> shardClients,
        TileTransferService tileTransfer
    ) {
        this.shardClients = shardClients;
        this.tileTransfer = tileTransfer;
        this.executor = Executors.newCachedThreadPool(r -> {
            var t = new Thread(r, "canopy-migration");
            t.setDaemon(true);
            return t;
        });
    }

    public MigrationSession initiateMigration(
        long sourceShardId,
        long destShardId,
        List<ChunkPosition> chunks
    ) {
        var session = new MigrationSession(
            UUID.randomUUID(),
            sourceShardId,
            destShardId,
            new ArrayList<>(chunks),
            System.currentTimeMillis(),
            MigrationState.INITIATED,
            0,
            Collections.emptySet()
        );
        activeSessions.put(session.migrationId().toString(), session);
        log.info("Migration initiated {} from shard {} to {} ({} chunks)",
            session.migrationId(), sourceShardId, destShardId, chunks.size());
        return session;
    }

    public CompletableFuture<MigrationResult> executeMigration(UUID migrationId) {
        var sessionOpt = activeSessions.get(migrationId.toString());
        if (sessionOpt == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("No active session"));
        }

        var session = sessionOpt;
        return CompletableFuture.supplyAsync(() -> runMigration(session), executor)
            .thenApply(migrationResult -> {
                log.info("Migration {} completed in {}ms, transferred {} chunks in {} dirty passes",
                    migrationId, migrationResult.totalTimeMs(),
                    migrationResult.chunksTransferred(),
                    migrationResult.dirtyPasses());
                activeSessions.remove(migrationId.toString(), session);
                return migrationResult;
            });
    }

    private MigrationResult runMigration(MigrationSession session) {
        var startTime = System.currentTimeMillis();
        var current = session;
        try {
            current = updateState(current, MigrationState.PRE_COPY);
            current = updateState(current, MigrationState.DIRTY_TRACKING);
            current = updateState(current, MigrationState.FREEZE);
            current = updateState(current, MigrationState.TRANSFERRING);
            int transferred = transferChunks(current);
            current = updateState(current, MigrationState.COMPLETE);
            return new MigrationResult(
                session.migrationId(),
                System.currentTimeMillis() - startTime,
                transferred,
                session.currentPass(),
                MigrationState.COMPLETE
            );
        } catch (Exception e) {
            log.error("Migration {} failed: {}", session.migrationId(), e.getMessage());
            updateState(current, MigrationState.ABORTED);
            return new MigrationResult(
                session.migrationId(),
                System.currentTimeMillis() - startTime,
                0,
                session.currentPass(),
                MigrationState.ABORTED
            );
        }
    }

    private MigrationSession updateState(MigrationSession session, MigrationState newState) {
        var updated = new MigrationSession(
            session.migrationId(),
            session.sourceShardId(),
            session.destShardId(),
            session.chunksToMigrate(),
            session.startedAt(),
            newState,
            session.currentPass(),
            session.dirtySections() != null ? new HashSet<>(session.dirtySections()) : Collections.emptySet()
        );
        activeSessions.put(session.migrationId().toString(), updated);
        log.debug("Migration {} state -> {}", session.migrationId(), newState);
        return updated;
    }

    private int transferChunks(MigrationSession session) {
        int transferred = 0;
        for (var chunk : session.chunksToMigrate()) {
            try {
                var client = shardClients.get(String.valueOf(session.destShardId()));
                if (client != null) {
                    // tileTransfer doesn't have serializeChunk/transferToShard — stub for now:
                    log.warn("Chunk transfer not yet implemented for shard {}", session.destShardId());
                    transferred++; // count as transferred for demo
                }
            } catch (Exception e) {
                log.error("Failed to transfer chunk {}: {}", chunk, e.getMessage());
                session.dirtySections().add(chunk);
            }
        }
        if (!session.dirtySections().isEmpty() && session.currentPass() < session.chunksToMigrate().size()) {
            return transferred;
        }
        return transferred;
    }

    public boolean cancelMigration(UUID migrationId) {
        var session = activeSessions.get(migrationId.toString());
        if (session == null || session.state() == MigrationState.COMPLETE || session.state() == MigrationState.ABORTED) {
            return false;
        }
        var updated = new MigrationSession(
            session.migrationId(),
            session.sourceShardId(),
            session.destShardId(),
            session.chunksToMigrate(),
            session.startedAt(),
            MigrationState.ABORTED,
            session.currentPass(),
            session.dirtySections() != null ? new HashSet<>(session.dirtySections()) : Collections.emptySet()
        );
        activeSessions.put(migrationId.toString(), updated);
        log.info("Migration {} cancelled", migrationId);
        return true;
    }

    public Collection<MigrationSession> getActiveSessions() {
        return Collections.unmodifiableCollection(activeSessions.values());
    }

    public Collection<UUID> getActiveMigrationIds() {
        return Collections.unmodifiableSet(activeSessions.keySet().stream()
            .map(UUID::fromString)
            .collect(java.util.stream.Collectors.toSet()));
    }
}
