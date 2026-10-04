package com.folksypizza.canopy.session.runtime;

import com.folksypizza.canopy.migration.PlayerStateCodec;
import com.folksypizza.canopy.session.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.event.*;
import org.bukkit.event.entity.*;
import org.bukkit.event.inventory.*;
import org.bukkit.event.player.*;
import org.bukkit.plugin.java.JavaPlugin;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.*;

/** Opt-in backend session adapter. Online presence comes from CSG, never a backend quit event. */
public final class SessionRuntime implements Listener, AutoCloseable {
    private final JavaPlugin plugin;
    private final SessionAudit audit;
    private final String shard;
    private volatile boolean stopped;
    private final HikariDataSource database;
    private final JdbcSessionStore store;
    private final SessionCoordinator coordinator;
    private final GatewayLeaseStore gateways;
    private final JdbcOwnershipDirectory ownership;
    private final BackendLeaseStore backends;
    private final String instance = UUID.randomUUID().toString();
    private volatile boolean storageHeld;
    private final ScheduledExecutorService leaseTimer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "canopy-backend-lease"); t.setDaemon(true); return t;
    });
    private final ConcurrentHashMap<UUID, PlayerSession> arrivals = new ConcurrentHashMap<>();
    private final SessionControlServer server;
    private final ConcurrentHashMap<UUID, Local> players = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor io = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(128), task -> { Thread t = new Thread(task, "canopy-session-io"); t.setDaemon(true); return t; });

    private static final class Local {
        final Player player;
        volatile PlayerSession.AuthorityToken authority;
        volatile boolean held = true, restoring, applied, departed;
        volatile byte[] latest;
        volatile long sequence;
        volatile Location landing;
        volatile Captured cutSnapshot;
        volatile CompletableFuture<Captured> cut = new CompletableFuture<>();
        volatile SpawnReference spawn;
        volatile PlayerSession.Life life = PlayerSession.Life.ALIVE;
        volatile long lifeRevision;
        volatile CompletableFuture<Void> lifeChange = CompletableFuture.completedFuture(null);
        Local(Player player) {
            this.player = player;
        }
    }

    private record Captured(PlayerSession.AuthorityToken authority, long sequence, byte[] bytes) {}

    public SessionRuntime(JavaPlugin plugin) throws Exception {
        this.plugin = plugin;
        var config = plugin.getConfig();
        shard = config.getString("session.shard", "");
        if (shard == null || shard.isBlank()) throw new IllegalArgumentException("session.shard is required");
        String secret = config.getString("session.shared-secret", "");
        if (secret == null || secret.length() < 16) throw new IllegalArgumentException("session.shared-secret is required");
        HikariConfig db = new HikariConfig();
        // Paper isolates plugin class loaders; DriverManager discovery alone cannot see this bundled driver.
        db.setDriverClassName("org.mariadb.jdbc.Driver");
        db.setJdbcUrl(config.getString("session.database.url", ""));
        db.setUsername(config.getString("session.database.user", ""));
        db.setPassword(config.getString("session.database.password", ""));
        db.setMaximumPoolSize(8); db.setMinimumIdle(0);
        db.setConnectionTimeout(2000); db.setValidationTimeout(1000); db.setInitializationFailTimeout(1);
        db.addDataSourceProperty("connectTimeout", "2000"); db.addDataSourceProperty("socketTimeout", "4000");
        var auditConfig = new java.util.Properties();
        for (String key : java.util.List.of("enabled", "max-bytes", "retained-files", "queue-capacity")) {
            Object value = config.get("session.audit." + key);
            if (value != null) auditConfig.setProperty("audit." + key, value.toString());
        }
        try { audit = SessionAudit.configured(plugin.getDataFolder().toPath(), "backend:" + shard,
            auditConfig, message -> plugin.getLogger().warning(message)); }
        catch (Exception failed) { io.shutdownNow(); throw failed; }
        try { database = new HikariDataSource(db); }
        catch (Exception failed) { audit.failure("storage.start_failed", java.util.Map.of("owner", shard), failed); io.shutdownNow(); audit.close(); throw failed; }
        store = new JdbcSessionStore(database); coordinator = new SessionCoordinator(store, audit);
        gateways = new GatewayLeaseStore(database);
        ownership = new JdbcOwnershipDirectory(database);
        backends = new BackendLeaseStore(database);
        SessionControlServer endpoint = null;
        try {
            store.initialize(); gateways.initialize(); ownership.initialize();
            backends.initialize(); backends.claim(shard, instance);
            audit.event("lease.claimed", java.util.Map.of("owner", shard, "instance", instance));
            for (String entry : config.getStringList("session.owned-chunks")) {
                String[] parts = entry.split(",");
                if (parts.length != 3) throw new IllegalArgumentException("Owned chunks use world,chunkX,chunkZ");
                ownership.seed(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), shard, 1);
            }
            endpoint = new SessionControlServer(new InetSocketAddress(config.getString("session.bind", "127.0.0.1"),
                config.getInt("session.port", 50151)), secret, this::execute, audit);
            registerSpawnHook();
        } catch (Exception failure) {
            audit.failure("backend.start_failed", java.util.Map.of("owner", shard), failure);
            if (endpoint != null) endpoint.close(); database.close(); io.shutdownNow(); audit.close(); throw failure;
        }
        server = endpoint;
        leaseTimer.scheduleWithFixedDelay(() -> {
            if (stopped) return;
            try {
                backends.heartbeat(shard, instance);
                for (var entry : players.entrySet()) {
                    Local local = entry.getValue();
                    if (local.authority == null || local.lifeChange.isDone() == false) continue;
                    PlayerSession current = store.find(entry.getKey()).orElse(null);
                    boolean owned = current != null && current.phase() == PlayerSession.Phase.ACTIVE
                        && shard.equals(current.owner()) && current.gatewayToken().equals(local.authority.gateway());
                    // Life/cancel epochs are reconciled by their acknowledged transition, not a racing lease read.
                    if (!owned && !local.restoring) local.held = true;
                    if (local.departed && !owned) players.remove(entry.getKey(), local);
                }
                if (storageHeld) audit.event("storage.recovered", java.util.Map.of("owner", shard));
                storageHeld = false;
            } catch (Exception unavailable) {
                audit.failure(storageHeld ? "storage.retry_failed" : "storage.held", java.util.Map.of("owner", shard), unavailable);
                storageHeld = true;
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /** Publish control readiness only after the plugin and lifecycle listeners are initialized. */
    public void start() { server.start(); audit.event("backend.ready", java.util.Map.of("owner", shard, "instance", instance)); }

    private PlayerSession execute(SessionCommand command) throws Exception {
        if (command.operation() == SessionCommand.Operation.HEARTBEAT) {
            gateways.heartbeat(command.gatewayId()); return null;
        }
        if (command.operation() == SessionCommand.Operation.OPEN) {
            PlayerSession old = store.find(command.playerId()).orElse(null);
            if (old != null && old.phase() != PlayerSession.Phase.OFFLINE && !old.sessionId().equals(command.sessionId()) && !gateways.alive(old.gatewayId())) {
                if (old.owner() != null && !shard.equals(old.owner()) && backends.alive(old.owner()))
                    throw new SessionCoordinator.Conflict("Expired gateway recovery requires the live owner");
                if (shard.equals(old.owner())) logout(SessionCommand.gateway(SessionCommand.Operation.DISCONNECT, old.gatewayToken()), old);
                else { coordinator.disconnect(old.gatewayToken()); coordinator.finishFromCheckpoint(old.gatewayToken()); }
            }
            gateways.heartbeat(command.gatewayId());
            return coordinator.open(command.playerId(), command.sessionId(), command.gatewayId());
        }
        PlayerSession state = store.find(command.playerId()).orElseThrow(() -> new SessionCoordinator.Conflict("Unknown session"));
        if (command.operation() == SessionCommand.Operation.FIND) return state;
        if (!state.gatewayToken().equals(command.token())) throw new SessionCoordinator.Conflict("Superseded session");
        return switch (command.operation()) {
            case ATTACH -> coordinator.attach(command.token(), shard);
            case PREPARE -> prepare(command, state);
            case ROUTE_RESPAWN -> {
                Local local = matching(state); local.lifeChange.get(3, TimeUnit.SECONDS);
                yield prepareRespawn(command, store.find(command.playerId()).orElseThrow());
            }
            case COMMIT -> commit(command, state);
            case ACTIVATE -> {
                Local local = matching(state);
                if (!local.applied) throw new SessionCoordinator.Conflict("State not applied yet");
                PlayerSession active = state;
                if (state.snapshot().length == 0) {
                    active = local.life == PlayerSession.Life.DEAD
                        ? coordinator.death(state.authorityToken(), local.sequence, local.latest)
                        : coordinator.checkpoint(state.authorityToken(), local.sequence, local.latest);
                    local.life = active.life(); local.lifeRevision = active.lifeRevision();
                }
                activate(active); yield active;
            }
            case CANCEL -> cancel(command, state);
            case DISCONNECT -> logout(command, state);
            case RECOVER_LOGOUT -> {
                if (state.phase() == PlayerSession.Phase.OFFLINE) yield state;
                if (state.owner() != null && backends.alive(state.owner()))
                    throw new SessionCoordinator.Conflict("Live owner must supply its final snapshot");
                coordinator.disconnect(command.token());
                audit.state("recovery.checkpoint_only", state);
                plugin.getLogger().warning("Finalizing a stopped backend session from its last durable checkpoint");
                yield coordinator.finishFromCheckpoint(command.token());
            }
            default -> throw new IllegalArgumentException("Unsupported session command");
        };
    }

    private PlayerSession prepare(SessionCommand command, PlayerSession state) throws Exception {
        if (!shard.equals(state.owner())) throw new SessionCoordinator.Conflict("Wrong source shard");
        if (state.phase() == PlayerSession.Phase.HANDOFF && command.transferId().equals(state.transferId())
            && command.target().equals(state.target())) return state;
        Local local = matching(state);
        if (state.epoch() != command.epoch()) throw new SessionCoordinator.Conflict("Stale input cut request");
        Captured snapshot = local.cut.get(3, TimeUnit.SECONDS);
        return coordinator.prepareHandover(snapshot.authority(), command.transferId(), command.target(), snapshot.sequence(), snapshot.bytes());
    }

    private PlayerSession prepareRespawn(SessionCommand command, PlayerSession state) throws Exception {
        if (!shard.equals(state.owner())) {
            throw new SessionCoordinator.Conflict("Respawn requires the current dead owner");
        }
        if (state.phase() == PlayerSession.Phase.HANDOFF && command.transferId().equals(state.transferId())) return state;
        if (state.phase() != PlayerSession.Phase.ACTIVE) throw new SessionCoordinator.Conflict("Respawn session is not active");
        if (state.snapshot().length == 0) throw new SessionCoordinator.Conflict("Initial state has not been activated");
        if (state.life() == PlayerSession.Life.ALIVE) return state;
        SpawnReference spawn = SessionSnapshot.decode(state.snapshot()).spawn();
        String target = spawn == null ? plugin.getConfig().getString("session.default-spawn-owner", shard)
            : ownership.owner(spawn.world(), spawn.x() >> 4, spawn.z() >> 4)
                .orElseThrow(() -> new SessionCoordinator.Conflict("Spawn block ownership unavailable"));
        if (target == null || target.isBlank()) target = shard;
        if (target.equals(shard)) return state;
        Local local = matching(state);
        local.held = true;
        return coordinator.prepareHandover(state.authorityToken(), command.transferId(), target,
            state.snapshotSequence() + 1, state.snapshot());
    }

    private PlayerSession commit(SessionCommand command, PlayerSession state) throws Exception {
        Local local = matching(state);
        if (!local.applied) throw new SessionCoordinator.Conflict("Destination is not applied");
        PlayerSession committed;
        if (state.phase() == PlayerSession.Phase.ACTIVE && shard.equals(state.owner())
            && state.epoch() >= command.epoch() + 1 && command.transferId().equals(state.transferId())) committed = state;
        else {
            if (!shard.equals(state.target())) throw new SessionCoordinator.Conflict("Wrong destination");
            committed = coordinator.commitHandover(command.token(), command.transferId(), command.epoch(), local.latest);
        }
        activate(committed); return committed;
    }

    private PlayerSession cancel(SessionCommand command, PlayerSession state) throws Exception {
        if (!shard.equals(state.owner())) throw new SessionCoordinator.Conflict("Wrong source");
        PlayerSession resumed = state.phase() == PlayerSession.Phase.ACTIVE
            && state.epoch() >= command.epoch() + 1 && command.transferId().equals(state.transferId())
            ? state : state.phase() == PlayerSession.Phase.ACTIVE
                ? coordinator.cancelCut(new PlayerSession.AuthorityToken(command.token(), shard, command.epoch()), command.transferId())
                : coordinator.cancelHandover(command.token(), command.transferId(), command.epoch());
        Local local = matching(resumed);
        if (resumed.life() == PlayerSession.Life.DEAD) {
            local.authority = resumed.authorityToken(); local.sequence = 0;
            local.cutSnapshot = null; local.cut = new CompletableFuture<>(); release(local); local.landing = null;
            return resumed;
        }
        onPlayer(local, () -> {
            local.authority = resumed.authorityToken(); local.sequence = 0;
            local.cutSnapshot = null; local.cut = new CompletableFuture<>(); release(local); local.landing = null;
            local.player.sendPluginMessage(plugin, "canopy:cancelled", new byte[0]);
        }).get(3, TimeUnit.SECONDS);
        return resumed;
    }

    private PlayerSession logout(SessionCommand command, PlayerSession state) throws Exception {
        Local pending = players.get(state.playerId());
        if (pending != null) pending.lifeChange.get(3, TimeUnit.SECONDS);
        PlayerSession closing = coordinator.disconnect(command.token());
        if (closing.phase() == PlayerSession.Phase.OFFLINE) return closing;
        if (closing.owner() != null && !shard.equals(closing.owner())) throw new SessionCoordinator.Conflict("Wrong logout owner");
        Local local = players.get(state.playerId());
        if (closing.target() != null || local == null || local.authority == null) {
            return coordinator.finishFromCheckpoint(command.token());
        }
        if (!local.authority.gateway().equals(command.token())) throw new SessionCoordinator.Conflict("Superseded local player");
        Captured snapshot = local.departed
            ? new Captured(local.authority, local.sequence, local.latest)
            : local.life == PlayerSession.Life.DEAD
                ? new Captured(local.authority, ++local.sequence, local.latest) : capture(local, true).get(3, TimeUnit.SECONDS);
        if (snapshot.bytes() == null) throw new SessionCoordinator.Conflict("Final snapshot not available");
        if (snapshot.sequence() <= closing.snapshotSequence()) {
            return coordinator.finishFromCheckpoint(command.token());
        }
        PlayerSession offline = coordinator.finishLogout(snapshot.authority(), snapshot.sequence(), snapshot.bytes());
        if (local.departed) players.remove(state.playerId(), local);
        return offline;
    }

    private Local matching(PlayerSession state) {
        Local local = players.get(state.playerId());
        if (local == null || local.authority == null || !local.authority.gateway().equals(state.gatewayToken())) {
            throw new SessionCoordinator.Conflict("Backend session not ready");
        }
        return local;
    }

    private CompletableFuture<Void> onPlayer(Local local, Runnable action) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (local.departed || !local.player.isOnline()) { done.completeExceptionally(new IllegalStateException("Player departed")); return done; }
        var task = local.player.getScheduler().run(plugin, ignored -> {
            try { action.run(); done.complete(null); } catch (Throwable failure) { done.completeExceptionally(failure); }
        }, () -> done.completeExceptionally(new IllegalStateException("Player retired")));
        if (task == null) done.completeExceptionally(new IllegalStateException("Player retired"));
        return done;
    }

    private CompletableFuture<Captured> capture(Local local, boolean freeze) {
        if (local.departed) return CompletableFuture.completedFuture(new Captured(local.authority, local.sequence, local.latest));
        CompletableFuture<Captured> result = new CompletableFuture<>();
        onPlayer(local, () -> {
            if (freeze && local.cutSnapshot != null) { result.complete(local.cutSnapshot); return; }
            if (!freeze && local.held) throw new SessionCoordinator.Conflict("Player held before snapshot capture");
            try {
                Location position = local.landing == null ? local.player.getLocation() : local.landing;
                byte[] bytes = snapshot(local, position, PlayerStateCodec.serialize(local.player, position));
                local.latest = bytes; local.sequence++;
                Captured captured = new Captured(local.authority, local.sequence, bytes);
                if (freeze) { hold(local); local.cutSnapshot = captured; }
                result.complete(captured);
            } catch (Exception e) { audit.failure("snapshot.capture_failed", localFields(local), e); result.completeExceptionally(e); }
        }).exceptionally(error -> { result.completeExceptionally(error); return null; });
        return result;
    }

    public void landing(Player player, Location location) {
        Local local = players.get(player.getUniqueId());
        if (local != null) local.landing = location.clone();
    }

    /** Runs directly at the ordered marker on the player's entity thread, before accepting another input. */
    public void markCut(Player player) {
        Local local = players.get(player.getUniqueId());
        if (local == null || local.authority == null || local.held || storageHeld || !local.applied || local.departed) return;
        try {
            Location at = local.landing == null ? player.getLocation() : local.landing;
            byte[] bytes = snapshot(local, at, PlayerStateCodec.serialize(player, at));
            local.latest = bytes; local.sequence++;
            local.cutSnapshot = new Captured(local.authority, local.sequence, bytes);
            hold(local); local.cut.complete(local.cutSnapshot);
            player.sendPluginMessage(plugin, com.folksypizza.canopy.routing.PlayerRespawnRepair.CHANNEL, new byte[]{1});
        } catch (Exception failed) { audit.failure("snapshot.cut_failed", localFields(local), failed); local.cut.completeExceptionally(failed); }
    }

    private void activate(PlayerSession state) throws Exception {
        if (state.phase() != PlayerSession.Phase.ACTIVE || !shard.equals(state.owner())) throw new SessionCoordinator.Conflict("Not authoritative");
        Local local = matching(state);
        if (!local.applied) throw new SessionCoordinator.Conflict("State not applied yet");
        if (state.life() == PlayerSession.Life.DEAD) {
            // Dead entities have retired schedulers; activation changes only session bookkeeping.
            synchronized (local) {
                validateActivation(local, state);
                local.authority = state.authorityToken(); local.sequence = state.snapshotSequence();
                local.life = state.life(); local.lifeRevision = state.lifeRevision(); release(local);
            }
            return;
        }
        onPlayer(local, () -> {
            synchronized (local) {
            validateActivation(local, state);
            if (local.authority.epoch() != state.epoch() || !shard.equals(local.authority.owner())) {
                local.authority = state.authorityToken(); local.sequence = 0;
            }
            local.cutSnapshot = null;
            local.cut = new CompletableFuture<>(); local.landing = null;
            release(local);
            local.player.sendPluginMessage(plugin, "canopy:ready", new byte[0]);
            }
        }).get(3, TimeUnit.SECONDS);
    }

    private void validateActivation(Local local, PlayerSession state) {
        if (!local.lifeChange.isDone() || local.authority.epoch() > state.epoch()
                || local.lifeRevision > state.lifeRevision())
            throw new SessionCoordinator.Conflict("Activation was superseded by a life transition");
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void join(PlayerJoinEvent event) {
        Local local = new Local(event.getPlayer());
        audit.event("backend.join", localFields(local));
        players.put(event.getPlayer().getUniqueId(), local); hold(local);
        event.joinMessage(null);
        PlayerSession arriving = arrivals.remove(local.player.getUniqueId());
        if (arriving != null) {
            local.authority = new PlayerSession.AuthorityToken(arriving.gatewayToken(), shard, arriving.epoch());
            local.sequence = arriving.phase() == PlayerSession.Phase.ACTIVE ? arriving.snapshotSequence() : 0;
            local.life = arriving.life(); local.lifeRevision = arriving.lifeRevision();
            if (arriving.snapshot().length == 0) {
                try { bootstrap(local); }
                catch (Exception invalid) { audit.failure("bootstrap.failed", localFields(local), invalid); plugin.getLogger().warning("Initial native player state could not be imported"); }
            } else if (arriving.life() == PlayerSession.Life.DEAD) {
                try { restorePlayer(local, arriving.snapshot(), SessionSnapshot.decode(arriving.snapshot())); }
                catch (Exception invalid) { audit.failure("restore.dead_failed", localFields(local), invalid); plugin.getLogger().warning("Dead arrival could not restore"); }
            } else submit(() -> { try { restore(local, arriving); } catch (Exception invalid) { audit.failure("restore.alive_failed", localFields(local), invalid); plugin.getLogger().warning("Alive arrival could not restore"); } });
        } else submit(() -> {
            try {
                PlayerSession state = store.find(local.player.getUniqueId()).orElseThrow();
                if (!state.online() || !(shard.equals(state.owner()) || shard.equals(state.target()))) return;
                local.authority = new PlayerSession.AuthorityToken(state.gatewayToken(), shard, state.epoch());
                local.sequence = state.phase() == PlayerSession.Phase.ACTIVE ? state.snapshotSequence() : 0;
                local.life = state.life(); local.lifeRevision = state.lifeRevision();
                restore(local, state);
            } catch (Exception error) { audit.failure("restore.held", localFields(local), error); plugin.getLogger().warning("Managed player restoration is held: " + error.getClass().getSimpleName()); }
        });
        local.player.getScheduler().runAtFixedRate(plugin, task -> {
            if (local.departed) { task.cancel(); return; }
            if (local.held || local.authority == null || !local.applied) return;
            capture(local, false).thenAccept(captured -> {
                submit(() -> {
                    try { coordinator.checkpoint(captured.authority(), captured.sequence(), captured.bytes()); }
                    catch (SessionCoordinator.Conflict stale) { audit.failure("snapshot.stale", localFields(local), stale); }
                    catch (Exception unavailable) { audit.failure("snapshot.save_failed", localFields(local), unavailable); }
                });
            });
        }, () -> {}, 100, 100);
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void preLogin(AsyncPlayerPreLoginEvent event) {
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) return;
        try {
            PlayerSession state = store.find(event.getUniqueId()).orElseThrow();
            if (!state.online() || !(shard.equals(state.owner()) || shard.equals(state.target()))) throw new IllegalStateException();
            arrivals.put(event.getUniqueId(), state);
        } catch (Exception unavailable) {
            audit.failure("backend.admission_denied", java.util.Map.of("player", event.getUniqueId()), unavailable);
            event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                net.kyori.adventure.text.Component.text("Player state is unavailable. Please try again."));
        }
    }

    /** Use the asynchronous Paper hook when available; retain a capability-gated legacy fallback. */
    @SuppressWarnings("unchecked")
    private void registerSpawnHook() {
        try {
            Class<? extends Event> type = (Class<? extends Event>) Class.forName(
                "io.papermc.paper.event.player.AsyncPlayerSpawnLocationEvent", false, plugin.getClass().getClassLoader());
            plugin.getServer().getPluginManager().registerEvent(type, this, EventPriority.HIGHEST, (listener, event) -> {
                try {
                    Object connection = type.getMethod("getConnection").invoke(event);
                    Class<?> connectionType = Class.forName("io.papermc.paper.connection.PlayerConfigurationConnection",
                        false, plugin.getClass().getClassLoader());
                    var profile = (com.destroystokyo.paper.profile.PlayerProfile)
                        connectionType.getMethod("getProfile").invoke(connection);
                    Location at = arrivingLocation(profile.getId());
                    if (at != null) type.getMethod("setSpawnLocation", Location.class).invoke(event, at);
                } catch (ReflectiveOperationException invalid) { throw new EventException(invalid); }
            }, plugin);
        } catch (ClassNotFoundException olderPaper) {
            plugin.getServer().getPluginManager().registerEvent(org.spigotmc.event.player.PlayerSpawnLocationEvent.class,
                this, EventPriority.HIGHEST, (listener, raw) -> {
                    var event = (org.spigotmc.event.player.PlayerSpawnLocationEvent) raw;
                    Location at = arrivingLocation(event.getPlayer().getUniqueId());
                    if (at != null) event.setSpawnLocation(at);
                }, plugin);
        }
    }

    private Location arrivingLocation(UUID player) {
        PlayerSession state = arrivals.get(player);
        if (state == null || state.snapshot().length == 0) return null;
        try {
            SessionSnapshot snapshot = SessionSnapshot.decode(state.snapshot());
            World world = Bukkit.getWorld(snapshot.world());
            Location at = world == null ? null : PlayerStateCodec.location(snapshot.playerState(), world);
            if (at == null) throw new IllegalArgumentException("Arrival location unavailable");
            return at;
        } catch (Exception invalid) { throw new IllegalStateException("Managed spawn could not resolve current state", invalid); }
    }

    private void restore(Local local, PlayerSession state) throws Exception {
        byte[] bytes = state.snapshot();
        if (bytes.length == 0) {
            onPlayer(local, () -> {
                bootstrap(local);
            }).get(3, TimeUnit.SECONDS); return;
        }
        SessionSnapshot snapshot = SessionSnapshot.decode(bytes);
        onPlayer(local, () -> ((com.folksypizza.canopy.CanopyPlugin) plugin).getRespawnRepair()
            .awaitGateway(local.player, true).thenRun(() -> restorePlayer(local, bytes, snapshot)).exceptionally(error -> {
                local.restoring = false;
                audit.failure("restore.held", localFields(local), error); plugin.getLogger().warning("Managed arrival is held because restoration failed");
                return null;
            })).get(3, TimeUnit.SECONDS);
    }

    private void bootstrap(Local local) {
        try {
            Location spawn = local.player.getRespawnLocation(false);
            if (spawn != null) {
                Object handle = local.player.getClass().getMethod("getHandle").invoke(local.player);
                boolean forced;
                try {
                    Object nativeSpawn = handle.getClass().getMethod("getRespawnConfig").invoke(handle);
                    forced = nativeSpawn != null && (boolean) nativeSpawn.getClass().getMethod("forced").invoke(nativeSpawn);
                } catch (NoSuchMethodException olderPaper) {
                    forced = (boolean) handle.getClass().getMethod("isRespawnForced").invoke(handle);
                }
                local.spawn = new SpawnReference(spawn.getWorld().getName(), spawn.getBlockX(), spawn.getBlockY(),
                    spawn.getBlockZ(), spawn.getYaw(), spawn.getPitch(), forced);
            }
            local.life = local.player.isDead() ? PlayerSession.Life.DEAD : PlayerSession.Life.ALIVE;
            local.latest = snapshot(local, local.player.getLocation(), PlayerStateCodec.serialize(local.player));
            local.sequence++; local.applied = true;
            audit.event("bootstrap.applied", localFields(local));
        } catch (Exception invalid) { throw new IllegalStateException("Initial player state unavailable", invalid); }
    }

    private void restorePlayer(Local local, byte[] bytes, SessionSnapshot snapshot) {
        World world = Bukkit.getWorld(snapshot.world());
        if (world == null) throw new IllegalStateException("Snapshot dimension unavailable");
        local.restoring = true;
        local.spawn = snapshot.spawn();
        applySpawn(local);
        Location at = PlayerStateCodec.apply(local.player, snapshot.playerState(), world,
            arrival -> { if (local.life != PlayerSession.Life.DEAD)
                ((com.folksypizza.canopy.CanopyPlugin) plugin).getRespawnRepair().beforeRestore(local.player, arrival); },
            local.life == PlayerSession.Life.DEAD);
        if (at == null) { local.restoring = false; throw new IllegalStateException("Player state invalid"); }
        if (local.life == PlayerSession.Life.DEAD) {
            local.latest = destinationSnapshot(snapshot); local.applied = true; local.restoring = false; audit.event("restore.applied", positionFields(local, at)); return;
        }
        local.player.teleportAsync(at).whenComplete((ok, error) -> {
            onPlayer(local, () -> {
                if (error != null || !Boolean.TRUE.equals(ok)) { audit.event("restore.teleport_failed", localFields(local)); local.restoring = false; return; }
                var velocity = PlayerStateCodec.readVelocity(snapshot.playerState());
                if (velocity != null) local.player.setVelocity(velocity);
                local.latest = destinationSnapshot(snapshot); local.applied = true; local.restoring = false;
                audit.event("restore.applied", positionFields(local, at));
            });
        });
    }

    private byte[] destinationSnapshot(SessionSnapshot snapshot) {
        try { return new SessionSnapshot(snapshot.world(), shard, snapshot.playerState(), snapshot.spawn()).encode(); }
        catch (java.io.IOException invalid) { throw new IllegalStateException("Destination envelope invalid", invalid); }
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void quit(PlayerQuitEvent event) {
        Local local = players.get(event.getPlayer().getUniqueId());
        if (local == null) return;
        event.quitMessage(null);
        if (!local.held && local.authority != null) {
            try {
                if (local.life != PlayerSession.Life.DEAD)
                    local.latest = snapshot(local, local.player.getLocation(), PlayerStateCodec.serialize(local.player));
                local.sequence++;
            } catch (Exception invalid) { audit.failure("snapshot.quit_capture_failed", localFields(local), invalid); }
        }
        audit.event("backend.quit", localFields(local));
        local.departed = true;
        cleanup(local);
        // A quit checkpoint must not outrun the death/respawn transaction using the old epoch.
        local.lifeChange.whenComplete((ignored, lifeError) -> {
            if (lifeError != null) return;
            submit(() -> {
            try {
                if (local.authority == null || local.latest == null) return;
                PlayerSession state = store.find(local.player.getUniqueId()).orElseThrow();
                if (!state.gatewayToken().equals(local.authority.gateway()) || !shard.equals(state.owner())) return;
                if (state.phase() == PlayerSession.Phase.CLOSING && state.target() == null) {
                    coordinator.finishLogout(local.authority, local.sequence, local.latest);
                    players.remove(local.player.getUniqueId(), local);
                } else if (state.phase() == PlayerSession.Phase.ACTIVE) {
                    coordinator.checkpoint(local.authority, local.sequence, local.latest);
                }
            } catch (Exception retryable) { audit.failure("snapshot.quit_save_failed", localFields(local), retryable); }
        });
        });
    }

    private boolean submit(Runnable work) {
        try { io.execute(work); return true; }
        catch (RejectedExecutionException busy) { audit.failure("backend.queue_rejected", java.util.Map.of("queued", io.getQueue().size()), busy); return false; }
    }

    private byte[] snapshot(Local local, Location position, byte[] state) throws java.io.IOException {
        return new SessionSnapshot(position.getWorld().getName(), shard, state, local.spawn).encode();
    }

    private void applySpawn(Local local) {
        SpawnReference spawn = local.spawn;
        World world = spawn == null ? null : Bukkit.getWorld(spawn.world());
        if (spawn != null && world == null) throw new IllegalStateException("Global spawn dimension unavailable");
        local.player.setRespawnLocation(spawn == null ? null
            : new Location(world, spawn.x(), spawn.y(), spawn.z(), spawn.yaw(), spawn.pitch()), spawn != null && spawn.forced());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void spawnSet(com.destroystokyo.paper.event.player.PlayerSetSpawnEvent event) {
        Local local = players.get(event.getPlayer().getUniqueId());
        if (local == null || local.restoring) return;
        Location at = event.getLocation();
        audit.event("spawn.changed", localFields(local));
        local.spawn = at == null ? null : new SpawnReference(at.getWorld().getName(), at.getBlockX(), at.getBlockY(),
            at.getBlockZ(), at.getYaw(), at.getPitch(), event.isForced());
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void quietRestoredDeath(PlayerDeathEvent event) {
        Local local = players.get(event.getPlayer().getUniqueId());
        if (local == null || !local.restoring) return;
        event.getDrops().clear(); event.setDroppedExp(0); event.setKeepInventory(true); event.setKeepLevel(true);
        event.deathMessage(null);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void death(PlayerDeathEvent event) {
        Local local = players.get(event.getPlayer().getUniqueId());
        if (local == null || local.restoring || local.held || local.authority == null) return;
        try {
            hold(local);
            byte[] bytes = snapshot(local, event.getPlayer().getLocation(), PlayerStateCodec.serializeDeath(event.getPlayer(), event));
            var token = local.authority; long sequence = ++local.sequence; long life = local.lifeRevision;
            local.latest = bytes;
            local.lifeChange = persistLife(local, token, sequence, bytes, life, true);
        } catch (Exception invalid) { audit.failure("life.death_held", localFields(local), invalid); plugin.getLogger().warning("Death outcome remains held for recovery"); }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void respawned(com.destroystokyo.paper.event.player.PlayerPostRespawnEvent event) {
        Local local = players.get(event.getPlayer().getUniqueId());
        if (local == null || local.restoring || local.life != PlayerSession.Life.DEAD) return;
        try {
            hold(local);
            byte[] bytes = snapshot(local, event.getRespawnedLocation(), PlayerStateCodec.serialize(event.getPlayer()));
            var token = local.authority; long sequence = ++local.sequence; long life = local.lifeRevision;
            local.latest = bytes;
            local.lifeChange = persistLife(local, token, sequence, bytes, life, false);
        } catch (Exception invalid) { audit.failure("life.respawn_held", localFields(local), invalid); plugin.getLogger().warning("Respawn remains held for recovery"); }
    }

    private CompletableFuture<Void> persistLife(Local local, PlayerSession.AuthorityToken token, long sequence,
                                                byte[] bytes, long life, boolean death) {
        CompletableFuture<Void> saved = new CompletableFuture<>();
        persistLifeAttempt(local, token, sequence, bytes, life, death, saved);
        return saved;
    }
    private void persistLifeAttempt(Local local, PlayerSession.AuthorityToken token, long sequence, byte[] bytes,
                                    long life, boolean death, CompletableFuture<Void> saved) {
        if (stopped) { saved.completeExceptionally(new IllegalStateException("Backend closing")); return; }
        boolean queued = submit(() -> {
            try {
                PlayerSession current = store.find(token.gateway().playerId()).orElseThrow();
                PlayerSession changed = current.gatewayToken().equals(token.gateway()) && current.epoch() == token.epoch() + 1
                    && current.lifeRevision() == life + 1 && current.life() == (death ? PlayerSession.Life.DEAD : PlayerSession.Life.ALIVE)
                    ? current : death ? coordinator.death(token, sequence, bytes) : coordinator.respawn(token, life, sequence, bytes);
                synchronized (local) {
                    if (local.authority.epoch() > changed.epoch()) throw new SessionCoordinator.Conflict("Life acknowledgement superseded");
                    local.authority = changed.authorityToken(); local.sequence = 0;
                    local.life = changed.life(); local.lifeRevision = changed.lifeRevision();
                }
                if (death || local.departed) { release(local); saved.complete(null); }
                else {
                    Location position = PlayerStateCodec.location(SessionSnapshot.decode(bytes).playerState(),
                        Bukkit.getWorld(SessionSnapshot.decode(bytes).world()));
                    String target = position == null ? null : ownership.owner(position.getWorld().getName(),
                        position.getBlockX() >> 4, position.getBlockZ() >> 4).orElse(null);
                    onPlayer(local, () -> {
                        release(local); saved.complete(null);
                        if (target != null && !target.equals(shard)) {
                            local.landing = position;
                            var out = com.google.common.io.ByteStreams.newDataOutput();
                            out.writeUTF(target); out.writeByte(1);
                            local.player.sendPluginMessage(plugin, "canopy:switch", out.toByteArray());
                        }
                    }).exceptionally(error -> { saved.completeExceptionally(error); return null; });
                }
            } catch (SessionCoordinator.Conflict conflict) {
                audit.failure("life.conflict", localFields(local), conflict);
                try {
                    PlayerSession current = store.find(token.gateway().playerId()).orElseThrow();
                    if (current.phase() == PlayerSession.Phase.ACTIVE && current.authorityToken().equals(token)
                            && current.lifeRevision() == life) {
                        retryLife(local, token, sequence, bytes, life, death, saved);
                    } else saved.completeExceptionally(conflict);
                } catch (Exception outage) { retryLife(local, token, sequence, bytes, life, death, saved); }
            }
            catch (Exception outage) {
                audit.failure("life.save_failed", localFields(local), outage);
                retryLife(local, token, sequence, bytes, life, death, saved);
            }
        });
        if (!queued) retryLife(local, token, sequence, bytes, life, death, saved);
    }
    private void retryLife(Local local, PlayerSession.AuthorityToken token, long sequence, byte[] bytes,
                           long life, boolean death, CompletableFuture<Void> saved) {
        if (stopped || !plugin.isEnabled()) { saved.completeExceptionally(new IllegalStateException("Backend closing")); return; }
        audit.event("life.retry", localFields(local));
        plugin.getServer().getAsyncScheduler().runDelayed(plugin,
            task -> persistLifeAttempt(local, token, sequence, bytes, life, death, saved), 1, TimeUnit.SECONDS);
    }
    private java.util.Map<String, Object> positionFields(Local local, Location at) {
        var fields = localFields(local); fields.put("world", at.getWorld().getName());
        fields.put("x", at.getX()); fields.put("y", at.getY()); fields.put("z", at.getZ());
        fields.put("yaw", at.getYaw()); fields.put("pitch", at.getPitch()); return fields;
    }

    private java.util.Map<String, Object> localFields(Local local) {
        var fields = new java.util.LinkedHashMap<String, Object>();
        fields.put("player", local.player.getUniqueId()); fields.put("owner", shard);
        var authority = local.authority;
        if (authority != null) {
            fields.put("session", authority.gateway().sessionId()); fields.put("gateway", authority.gateway().gatewayId());
            fields.put("epoch", authority.epoch());
        }
        fields.put("lifeRevision", local.lifeRevision); fields.put("life", local.life);
        fields.put("sequence", local.sequence);
        return fields;
    }
    private void hold(Local local) {
        if (!local.held) audit.event("input.held", localFields(local)); local.held = true;
    }
    private void cleanup(Local local) {
        if (local.held) audit.event("input.released", localFields(local)); local.held = false; local.restoring = false;
    }
    private void release(Local local) { cleanup(local); }
    private boolean held(Player player) { Local local = players.get(player.getUniqueId()); return local != null && (local.held || storageHeld) && !local.restoring; }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void move(PlayerMoveEvent e) { if (held(e.getPlayer())) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void damage(EntityDamageEvent e) { if (e.getEntity() instanceof Player p && held(p)) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void pickup(EntityPickupItemEvent e) { if (e.getEntity() instanceof Player p && held(p)) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void drop(PlayerDropItemEvent e) { if (held(e.getPlayer())) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void interact(PlayerInteractEvent e) { if (held(e.getPlayer())) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void interactEntity(PlayerInteractEntityEvent e) { if (held(e.getPlayer())) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void inventory(InventoryClickEvent e) { if (e.getWhoClicked() instanceof Player p && held(p)) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void drag(InventoryDragEvent e) { if (e.getWhoClicked() instanceof Player p && held(p)) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void consume(PlayerItemConsumeEvent e) { if (held(e.getPlayer())) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void shoot(EntityShootBowEvent e) { if (e.getEntity() instanceof Player p && held(p)) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void velocity(PlayerVelocityEvent e) { if (held(e.getPlayer())) e.setCancelled(true); }
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true) public void target(EntityTargetLivingEntityEvent e) { if (e.getTarget() instanceof Player p && held(p)) e.setCancelled(true); }

    @Override public void close() {
        audit.event("backend.stopping", java.util.Map.of("owner", shard));
        stopped = true;
        leaseTimer.shutdownNow();
        server.close();
        players.values().forEach(this::cleanup);
        players.clear();
        io.shutdown();
        try { io.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { backends.release(shard, instance); } catch (Exception unavailable) { audit.failure("lease.release_failed", java.util.Map.of("owner", shard), unavailable); }
        database.close();
        audit.close();
    }
}
