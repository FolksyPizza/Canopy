package com.folypizza.canopy.grpc;

import com.folypizza.canopy.proto.TileVersionServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * gRPC server for tile version queries and shard coordination.
 *
 * Intra-host communication uses shared memory/mmap for zero-copy,
 * lower-latency tile transfers.
 * Inter-host communication uses gRPC over TCP/loopback.
 *
 * Each Canopy shard instance runs one gRPC server (or shares one across
 * regions within that JVM).
 */
public class GrpcServer {
    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private final int port;
    private final TileVersionServiceImpl tileVersionService;
    private final ShardCoordinationServiceImpl coordinationService;
    private final MigrationServiceImpl migrationService;
    private Server server;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public GrpcServer(int port, TileVersionServiceImpl tileVersionService) {
        this(port, tileVersionService, null, null);
    }

    public GrpcServer(int port, TileVersionServiceImpl tileVersionService,
                      ShardCoordinationServiceImpl coordinationService,
                      MigrationServiceImpl migrationService) {
        this.port = port;
        this.tileVersionService = tileVersionService;
        this.coordinationService = coordinationService;
        this.migrationService = migrationService;
    }

    /**
     * Start the gRPC server.
     */
    public void start() {
        try {
            ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(TileVersionServiceGrpc.bindService(tileVersionService))
                .executor(scheduler);
            if (coordinationService != null) {
                builder.addService(coordinationService);
            }
            if (migrationService != null) {
                builder.addService(migrationService);
            }
            server = builder.build().start();

            running.set(true);
            log.info("gRPC server started on port {}", port);
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("gRPC server shutting down...");
                GrpcServer.this.stop();
            }));
        } catch (IOException e) {
            log.error("Failed to start gRPC server on port {}", port, e);
            running.set(false);
            throw new RuntimeException("Failed to start gRPC server", e);
        }
    }

    /**
     * Stop the gRPC server.
     */
    public void stop() {
        if (server != null) {
            server.shutdown();
            running.set(false);
        }
    }

    /**
     * Wait for the server to shut down (blocking).
     */
    public void blockUntilShutdown() {
        if (server != null) {
            try { server.awaitTermination(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * Gracefully shutdown the gRPC server.
     */
    public void gracefulShutdown() {
        if (server != null) {
            server.shutdownNow();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("gRPC server did not terminate in 5s, forcing shutdown");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        scheduler.shutdown();
        running.set(false);
        log.info("gRPC server shutdown complete");
    }

    public int getPort() {
        return server != null ? server.getPort() : port;
    }

    public boolean isRunning() {
        return running.get();
    }
}
