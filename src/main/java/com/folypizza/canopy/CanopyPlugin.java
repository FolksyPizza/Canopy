package com.folypizza.canopy;

import com.folypizza.canopy.entity.EntityTracker;
import com.folypizza.canopy.graph.PartitionAdvisor;
import com.folypizza.canopy.grpc.GrpcServer;
import com.folypizza.canopy.grpc.MigrationServiceImpl;
import com.folypizza.canopy.grpc.PeerManager;
import com.folypizza.canopy.grpc.ShardCoordinationServiceImpl;
import com.folypizza.canopy.grpc.TileVersionServiceImpl;
import com.folypizza.canopy.halo.TileUpdateTracker;
import com.folypizza.canopy.halo.TileVersionStore;
import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.lease.OwnershipLeaseService;
import com.folypizza.canopy.metrics.MetricsCollector;
import com.folypizza.canopy.migration.MigrationService;
import com.folypizza.canopy.redis.RedisLeaseService;
import com.folypizza.canopy.redis.RedisPartitionSyncer;
import com.folypizza.canopy.redis.RedisPeerService;
import com.folypizza.canopy.registry.ShardRegistry;
import com.folypizza.canopy.repartition.AutoRepartitionEngine;
import com.folypizza.canopy.routing.BoundaryTransferListener;
import com.folypizza.canopy.routing.PlayerRoutingProxy;
import com.folypizza.canopy.routing.SeamVisualizer;
import com.folypizza.canopy.scheduler.BlockUpdateHook;
import com.folypizza.canopy.scheduler.FoliaTickHook;
import com.folypizza.canopy.scheduler.MigrationScheduler;
import com.folypizza.canopy.transfer.TileTransferService;
import com.folypizza.canopy.world.WorldEventListener;
import com.folypizza.canopy.world.WorldPartitionAdapter;
import io.lettuce.core.RedisClient;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CanopyPlugin extends JavaPlugin {
    private static final Logger log = LoggerFactory.getLogger(CanopyPlugin.class);
    private static final int TELEM_INTERVAL_MS = 10_000;

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final ScheduledExecutorService globalScheduler = Executors.newScheduledThreadPool(
        2, r -> { Thread t = new Thread(r, "canopy-global"); t.setDaemon(true); return t; }
    );

    private GrpcServer grpcServer;
    private MetricsEndpoint metricsEndpoint;

    private TileVersionStore tileVersionStore;
    private TileUpdateTracker tileUpdateTracker;
    private TileVersionServiceImpl tileVersionService;
    private PartitionMap partitionMap;
    private MetricsCollector metricsCollector;
    private ShardRegistry shardRegistry;
    private TileTransferService tileTransferService;
    private MigrationService migrationService;
    private WorldPartitionAdapter worldAdapter;
    private BlockUpdateHook blockHook;
    private WorldEventListener worldEventListener;
    private EntityTracker entityTracker;
    private PlayerRoutingProxy routingProxy;
    private FoliaTickHook tickHook;
    private PartitionAdvisor partitionAdvisor;
    private AutoRepartitionEngine repartitionEngine;
    private MigrationScheduler migrationScheduler;
    private OwnershipLeaseService leaseService;
    private PeerManager peerManager;
    private BoundaryTransferListener boundaryTransferListener;
    private com.folypizza.canopy.routing.PlayerStateInbox playerStateInbox;
    private com.folypizza.canopy.halo.HaloEditStore haloEditStore;
    // Resolved transfer/seam parameters (read once in initCoreServices).
    private double transferBoundaryX;
    private boolean transferOwnsWest;
    private int transferHaloWidth;
    // Cached local overworld time (updated on the global region thread) for cross-shard sync.
    private final java.util.concurrent.atomic.AtomicLong localWorldTime = new java.util.concurrent.atomic.AtomicLong(0);

    // Optional Redis-backed coordination (only when lease.redis-url is configured).
    private RedisClient redisClient;
    private RedisLeaseService redisLeaseService;
    private RedisPartitionSyncer redisPartitionSyncer;
    private RedisPeerService redisPeerService;

    private long shardId;

    @Override
    public void onEnable() {
        if (!enabled.compareAndSet(false, true)) {
            log.warn("Canopy already enabled");
            return;
        }

        long start = System.currentTimeMillis();
        log.info("Canopy starting...");

        saveDefaultConfig();
        createConfigDirectory();

        try {
            initCoreServices();
            initNetworkEndpoints();
            initBackgroundServices();
        } catch (Exception e) {
            log.error("Failed to initialize Canopy", e);
            enabled.set(false);
            gracefulShutdown();
            return;
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info("Canopy enabled in {}ms", elapsed);
    }

    @Override
    public void onDisable() {
        log.info("Canopy disabling...");
        gracefulShutdown();
        enabled.set(false);
        log.info("Canopy disabled");
    }

    /**
     * Resolve this shard's id. An explicit numeric {@code shard.id} wins; otherwise a
     * unique id is derived from the host and gRPC port so co-located shards never
     * collide (a duplicate id would make a peer look like "self" and be skipped).
     */
    private long resolveShardId() {
        String idCfg = getConfig().getString("shard.id", "auto");
        if (idCfg != null && !idCfg.equalsIgnoreCase("auto") && !idCfg.isBlank()) {
            try {
                return Long.parseLong(idCfg.trim());
            } catch (NumberFormatException e) {
                log.warn("Invalid shard.id '{}' — falling back to derived id", idCfg);
            }
        }
        int grpcPort = getConfig().getInt("grpc.port", 50051);
        long hostHash = getHostAddress().hashCode() & 0xFFFFL;
        return (hostHash << 16) | (grpcPort & 0xFFFFL);
    }

    /** World border extent (blocks) along X; defaults to the vanilla max border. */
    public int getWorldBorderX() {
        return getConfig().getInt("shard.world-border-x", 30000000);
    }

    /** World border extent (blocks) along Z; defaults to the vanilla max border. */
    public int getWorldBorderZ() {
        return getConfig().getInt("shard.world-border-z", 30000000);
    }

    private void initCoreServices() {
        int numRegions = getConfig().getInt("shard.num-regions", 1);
        shardId = resolveShardId();

        tileVersionStore = new TileVersionStore(shardId);
        tileUpdateTracker = new TileUpdateTracker(shardId);
        partitionMap = new PartitionMap();
        PartitionMap.PartitionState initialState =
            PartitionMap.createDefault(numRegions, "shard", getWorldBorderX(), getWorldBorderZ()).build();
        partitionMap.apply(0, initialState.shards(), initialState.seams());
        metricsCollector = new MetricsCollector(shardId);
        shardRegistry = createShardRegistry(shardId);
        worldAdapter = new WorldPartitionAdapter(this, metricsCollector, partitionMap, tileUpdateTracker);
        blockHook = new BlockUpdateHook(partitionMap);
        entityTracker = new EntityTracker(shardId);
        routingProxy = new PlayerRoutingProxy(partitionMap);

        // Single shared tile-version service: the same instance is served over gRPC and
        // written to by world events, so remote halo queries see live local updates.
        tileVersionService = new TileVersionServiceImpl(
            shardId, tileVersionStore, tileUpdateTracker, partitionMap
        );

        tileTransferService = new TileTransferService(
            tileVersionService, partitionMap, tileUpdateTracker
        );

        java.util.concurrent.ConcurrentHashMap<String, com.folypizza.canopy.grpc.TileVersionClient> peerClients = new java.util.concurrent.ConcurrentHashMap<>();
        migrationService = new MigrationService(peerClients, tileTransferService);
        migrationScheduler = new MigrationScheduler();

        // Repartition engine + advisor (telemetry-driven; idles until imbalance appears).
        partitionAdvisor = new PartitionAdvisor(Math.max(1, numRegions), 0.5);
        repartitionEngine = new AutoRepartitionEngine(metricsCollector, partitionMap, partitionAdvisor);

        // Ownership lease coordination, Redis-backed when configured, in-memory otherwise.
        initLeaseAndRedis();

        // Resolve seam/transfer parameters once, shared by the listeners and peer manager.
        transferBoundaryX = getConfig().getDouble("transfer.boundary-x", 0);
        transferOwnsWest = !"east".equalsIgnoreCase(getConfig().getString("transfer.owns", "west"));
        transferHaloWidth = getConfig().getInt("transfer.halo-width", 16);
        haloEditStore = new com.folypizza.canopy.halo.HaloEditStore();
        playerStateInbox = new com.folypizza.canopy.routing.PlayerStateInbox();

        // World event capture: feeds tile versioning, entity tracking, routing, chunk counts,
        // and records near-seam block edits for halo mirroring.
        worldEventListener = new WorldEventListener(
            tileVersionService, worldAdapter, entityTracker, routingProxy, shardId,
            haloEditStore, transferBoundaryX, transferHaloWidth, transferOwnsWest);

        // Tick telemetry sampler (Folia global-region scheduled).
        tickHook = new FoliaTickHook(this, metricsCollector, partitionMap, shardId,
            entityTracker::getTrackedCount, worldEventListener::getLoadedChunkCount);

        // Peer cluster connectivity (gRPC to other shards listed in shard.peers).
        List<String> peers = getConfig().getStringList("shard.peers");
        peerManager = new PeerManager(shardId, peers, tileTransferService, shardRegistry, partitionMap,
            this, transferBoundaryX, transferHaloWidth, transferOwnsWest);

        log.info("Core services initialized (shard={}, regions={}, peers={})",
            shardId, numRegions, peers.size());
    }

    /**
     * Initialize the ownership lease layer. An in-memory {@link OwnershipLeaseService} is
     * always available; if {@code lease.redis-url} is set, Redis-backed coordination is
     * additionally started, falling back to in-memory on any connection failure.
     */
    private void initLeaseAndRedis() {
        leaseService = new OwnershipLeaseService();
        String redisUrl = getConfig().getString("lease.redis-url", "");
        if (redisUrl == null || redisUrl.isBlank()) {
            log.info("Redis not configured (lease.redis-url empty) — using in-memory coordination");
            return;
        }
        try {
            redisClient = RedisClient.create(redisUrl);
            redisLeaseService = new RedisLeaseService(redisClient);
            redisPartitionSyncer = new RedisPartitionSyncer(redisClient);
            redisPeerService = new RedisPeerService(redisClient, shardId);
            log.info("Redis coordination enabled at {}", redisUrl);
        } catch (Exception e) {
            log.warn("Failed to initialize Redis ({}) — falling back to in-memory coordination",
                e.getMessage());
            if (redisClient != null) {
                try { redisClient.shutdown(); } catch (Exception ignored) { }
            }
            redisClient = null;
            redisLeaseService = null;
            redisPartitionSyncer = null;
            redisPeerService = null;
        }
    }

    private void initNetworkEndpoints() {
        int grpcPort = getConfig().getInt("grpc.port", 50051);
        int metricsPort = getConfig().getInt("metrics.port", 8080);

        // The gRPC-exposed coordination + migration services share the plugin's live state.
        ShardCoordinationServiceImpl coordinationService = new ShardCoordinationServiceImpl(
            shardId, getHostAddress(), metricsCollector, entityTracker, routingProxy,
            partitionMap, tileVersionService, playerStateInbox, haloEditStore, localWorldTime::get);
        MigrationServiceImpl migrationGrpc = new MigrationServiceImpl(shardId, migrationService, this);

        grpcServer = new GrpcServer(grpcPort, tileVersionService, coordinationService, migrationGrpc);
        grpcServer.start();
        log.info("gRPC server started on port {} (tile-version, coordination, migration)", grpcPort);

        metricsEndpoint = new MetricsEndpoint(metricsPort, metricsCollector);
        metricsEndpoint.start();
        log.info("Metrics endpoint started on port {}", metricsPort);
    }



















    private void initBackgroundServices() {
        // Register world event capture — this is what makes the plugin react to the world.
        getServer().getPluginManager().registerEvents(worldEventListener, this);
        getServer().getPluginManager().registerEvents(blockHook, this);

        // Seamless cross-shard player movement.
        boolean transferEnabled = getConfig().getBoolean("transfer.enabled", false);
        String mode = getConfig().getString("transfer.mode", "transfer");
        String peerServer = getConfig().getString("transfer.peer-server", "");
        String peerHost = getConfig().getString("transfer.peer-host", "localhost");
        int peerPort = getConfig().getInt("transfer.peer-port", 25566);
        // Register the proxy plugin-message channel so we can request server switches.
        getServer().getMessenger().registerOutgoingPluginChannel(this, BoundaryTransferListener.SWITCH_CHANNEL);
        int buffer = getConfig().getInt("transfer.buffer", 4);
        boundaryTransferListener = new BoundaryTransferListener(
            this, transferEnabled, transferBoundaryX, buffer, transferOwnsWest, mode, peerServer, peerHost, peerPort,
            playerStateInbox, peerManager);
        getServer().getPluginManager().registerEvents(boundaryTransferListener, this);
        log.info("Event listeners registered (world events, seam corridor, boundary handover enabled={} mode={} owns={} boundaryX={} buffer={} haloWidth={} peer-server={})",
            transferEnabled, mode, transferOwnsWest ? "west" : "east", transferBoundaryX, buffer, transferHaloWidth, peerServer);

        // Cosmetic markers at the underlap band edges so the crossing is locatable in-world.
        boolean showSeam = getConfig().getBoolean("transfer.show-seam", true);
        getServer().getPluginManager().registerEvents(
            new SeamVisualizer(this, transferEnabled && showSeam, transferBoundaryX, buffer), this);

        // Keep non-player entities, items, and pearl teleports from crossing the seam.
        getServer().getPluginManager().registerEvents(
            new com.folypizza.canopy.routing.SeamContainmentListener(
                transferEnabled, transferBoundaryX, buffer, transferOwnsWest), this);

        // /region command (info, list, admin move).
        var regionCmd = new com.folypizza.canopy.routing.RegionCommand(
            shardId, transferBoundaryX, buffer, transferOwnsWest, shardRegistry, boundaryTransferListener);
        if (getCommand("region") != null) {
            getCommand("region").setExecutor(regionCmd);
        } else {
            log.warn("'region' command not declared in plugin.yml");
        }

        setupSpawn();

        tileTransferService.start();
        log.info("Tile transfer service started");

        tickHook.start();
        repartitionEngine.startMonitoring();

        if (redisPeerService != null) {
            redisPeerService.start();
            log.info("Redis peer service started");
        }

        peerManager.start();

        // Cross-shard world-time sync: the lowest shard id is the authority; other shards
        // follow it so day/night stays aligned across the seam. Runs on the global region
        // thread, which owns world time.
        getServer().getGlobalRegionScheduler().runAtFixedRate(this, task -> {
            if (getServer().getWorlds().isEmpty()) return;
            org.bukkit.World w = getServer().getWorlds().get(0);
            localWorldTime.set(w.getFullTime());
            long authTime = peerManager != null ? peerManager.getAuthorityWorldTime(shardId) : -1;
            if (authTime >= 0 && Math.abs(authTime - w.getFullTime()) > 20) {
                w.setFullTime(authTime);
            }
        }, 40L, 40L);

        globalScheduler.scheduleAtFixedRate(() -> {
            if (metricsCollector.getLoadedRegionCount() == 0) return;
            String snap;
            try { snap = metricsCollector.getMetricsSnapshot(); } catch (Exception e) { snap = "{}"; }
            log.debug("Telemetry: {} regions, {} metric bytes",
                metricsCollector.getLoadedRegionCount(), snap.length());
        }, TELEM_INTERVAL_MS, TELEM_INTERVAL_MS, TimeUnit.MILLISECONDS);

        log.info("Global telemetry started at {}ms interval", TELEM_INTERVAL_MS);
    }

    private void gracefulShutdown() {
        if (peerManager != null) { peerManager.stop(); }
        if (tickHook != null) { tickHook.stop(); }
        if (repartitionEngine != null) { repartitionEngine.stop(); }
        if (migrationScheduler != null) { migrationScheduler.shutdown(); }
        if (grpcServer != null) { grpcServer.stop(); }
        if (metricsEndpoint != null) { metricsEndpoint.stop(); }
        if (tileTransferService != null) { tileTransferService.stop(); }
        if (redisPeerService != null) { try { redisPeerService.stop(); } catch (Exception ignored) { } }
        if (leaseService != null) { leaseService.shutdown(); }
        if (shardRegistry != null) { shardRegistry.shutdown(); }
        if (redisClient != null) { try { redisClient.shutdown(); } catch (Exception ignored) { } }
        globalScheduler.shutdown();
        try { globalScheduler.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { globalScheduler.shutdownNow(); }
    }

    /**
     * Set the world spawn to (0,0) at terrain height so the single connection point drops
     * players at the origin. Runs on the region thread that owns the origin chunk.
     */
    private void setupSpawn() {
        if (getServer().getWorlds().isEmpty()) return;
        org.bukkit.World world = getServer().getWorlds().get(0);
        try {
            getServer().getRegionScheduler().run(this, world, 0, 0, task -> {
                try {
                    int y = world.getHighestBlockYAt(0, 0) + 1;
                    world.setSpawnLocation(0, y, 0, 0f);
                    log.info("World spawn set to (0,{},0) in {}", y, world.getName());
                } catch (Exception e) {
                    log.warn("Failed to set spawn at origin: {}", e.getMessage());
                }
            });
        } catch (Exception e) {
            log.warn("Could not schedule spawn setup: {}", e.getMessage());
        }
    }

    private ShardRegistry createShardRegistry(long localShardId) {
        var sr = new ShardRegistry();
        sr.register(new ShardRegistry.ShardEntry(
            localShardId, getHostAddress(),
            "localhost:" + getConfig().getInt("grpc.port", 50051),
            getConfig().getInt("shard.num-regions", 1)
        ));
        return sr;
    }

    private String getHostAddress() {
        try { return InetAddress.getLocalHost().getHostAddress(); }
        catch (Exception e) { return "127.0.0.1"; }
    }

    private void createConfigDirectory() {
        if (!getDataFolder().exists()) {
            getDataFolder().mkdirs();
        }
    }

    public TileVersionServiceImpl getTileVersionService() {
        return tileVersionService;
    }

    public EntityTracker getEntityTracker() {
        return entityTracker;
    }

    public PlayerRoutingProxy getRoutingProxy() {
        return routingProxy;
    }

    public OwnershipLeaseService getLeaseService() {
        return leaseService;
    }

    public AutoRepartitionEngine getRepartitionEngine() {
        return repartitionEngine;
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public PartitionMap getPartitionMap() {
        return partitionMap;
    }

    public TileTransferService getTileTransferService() {
        return tileTransferService;
    }

    public MigrationService getMigrationService() {
        return migrationService;
    }

    public WorldPartitionAdapter getWorldAdapter() {
        return worldAdapter;
    }

    public BlockUpdateHook getBlockHook() {
        return blockHook;
    }

    public TileVersionStore getTileVersionStore() {
        return tileVersionStore;
    }

    public TileUpdateTracker getTileUpdateTracker() {
        return tileUpdateTracker;
    }

    public ShardRegistry getShardRegistry() {
        return shardRegistry;
    }

    public ScheduledExecutorService getGlobalScheduler() {
        return globalScheduler;
    }
}
