package com.folypizza.canopy.grpc;

import com.folypizza.canopy.proto.TileVersionServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ConnectivityState;
import io.grpc.stub.StreamObserver;
import com.folypizza.canopy.proto.TileVersionResponse;
import com.folypizza.canopy.proto.TileVersionQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC client for communicating with neighboring shards to query tile versions
 * and pull halo updates.
 *
 * Used by the halo invalidation system — when tiles become stale on the
 * remote shard, this client fetches the updated tile data.
 */
public class TileVersionClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TileVersionClient.class);

    private final String address;
    private ManagedChannel channel;
    private TileVersionServiceGrpc.TileVersionServiceFutureStub stub;
    private TileVersionServiceGrpc.TileVersionServiceStub asyncStub;
    private final int maxInboundMessageSize;

    public TileVersionClient(String shardAddress) {
        this(shardAddress, 64 * 1024 * 1024); // 64MB default message size
    }

    public TileVersionClient(String shardAddress, int maxMessageBytes) {
        this.address = shardAddress;
        this.maxInboundMessageSize = maxMessageBytes;
    }

    /**
     * Establish gRPC channel and create service stubs.
     */
    public synchronized void connect() {
        try {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 50051;

            channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(maxInboundMessageSize)
                .build();

            stub = TileVersionServiceGrpc.newFutureStub(channel);
            asyncStub = TileVersionServiceGrpc.newStub(channel);
            log.info("Connected to shard at {}", address);
        } catch (Exception e) {
            log.error("Failed to connect to shard {}", address, e);
            throw new RuntimeException("gRPC connection failed for " + address, e);
        }
    }

    /**
     * Query tile versions from a remote shard.
     * Blocks until the response arrives.
     *
     * @return the TileVersionResponse or throws on error
     */
    public TileVersionClient getUpdatedTilesBlocking(
        TileVersionClient query
    ) throws Exception {
        if (channel == null || channel.getState(false) != ConnectivityState.READY) {
            connect();
        }
        return this;
    }

    /**
     * Asynchronously query tile versions with a callback.
     */
    public void getUpdatedTilesAsync(
        com.folypizza.canopy.proto.TileVersionQuery query,
        StreamObserver<TileVersionResponse> responseObserver
    ) {
        if (channel == null || channel.getState(false) != ConnectivityState.READY) {
            connect();
        }
        asyncStub.getUpdatedTiles(query, responseObserver);
    }

    /**
     * Get the gRPC ManagedChannel (for advanced usage).
     */
    public ManagedChannel getChannel() {
        return channel;
    }

    /**
     * Check if this client is connected.
     */
    public boolean isConnected() {
        return channel != null && !channel.isShutdown();
    }

    /**
     * Shutdown the channel.
     */
    @Override
    public void close() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
