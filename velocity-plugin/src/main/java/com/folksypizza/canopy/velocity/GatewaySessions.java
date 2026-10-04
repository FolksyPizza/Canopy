package com.folksypizza.canopy.velocity;

import com.folksypizza.canopy.session.*;
import com.velocitypowered.api.event.EventTask;
import com.velocitypowered.api.event.PostOrder;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.ResultedEvent;
import com.velocitypowered.api.event.connection.*;
import com.velocitypowered.api.event.player.*;
import com.velocitypowered.api.proxy.*;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** CSG owns presence; authenticated control calls survive the player's backend socket closing. */
public final class GatewaySessions implements AutoCloseable {
    private final ProxyServer proxy;
    private final Logger log;
    private final SessionAudit audit;
    private final SessionControlClient client;
    private final Map<String, URI> endpoints;
    private final String gateway;
    private final URI directory;
    private final ConcurrentHashMap<UUID, Connection> players = new ConcurrentHashMap<>();
    private final Set<Connection> closing = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor io = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(128), r -> { var t = new Thread(r, "canopy-gateway-control"); t.setDaemon(true); return t; });
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        var t = new Thread(r, "canopy-gateway-lifecycle"); t.setDaemon(true); return t;
    });
    private volatile boolean stopped;

    private static final class Connection {
        final Player player;
        final PlayerSession.GatewayToken token;
        volatile PlayerSession state;
        volatile boolean departed, switching;
        volatile boolean finalizing;
        volatile boolean respawning;
        Runnable respawnHook;
        Connection(Player player, PlayerSession state) { this.player = player; this.state = state; token = state.gatewayToken(); }
    }

    public static GatewaySessions load(ProxyServer proxy, Logger log, Path folder, SessionAudit audit) throws IOException {
        Path file = folder.resolve("session.properties");
        if (!Files.exists(file)) return null;
        Properties config = new Properties();
        try (var input = Files.newInputStream(file)) { config.load(input); }
        if (!Boolean.parseBoolean(config.getProperty("enabled", "false"))) return null;
        Map<String, URI> endpoints = new HashMap<>();
        for (String key : config.stringPropertyNames()) if (key.startsWith("backend.")) {
            String name = key.substring(8);
            if (proxy.getServer(name).isEmpty()) throw new IOException("Unknown session backend: " + name);
            URI uri = URI.create(config.getProperty(key));
            if ((!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) || uri.getHost() == null
                    || uri.getUserInfo() != null || uri.getQuery() != null) throw new IOException("Invalid session endpoint");
            endpoints.put(name, uri);
        }
        String directory = config.getProperty("directory", "");
        if (!endpoints.containsKey(directory)) throw new IOException("Session directory must name a configured backend");
        String id = config.getProperty("gateway-id", "");
        if (id.isBlank() || id.length() > 80) throw new IOException("A bounded gateway-id is required");
        try { Class.forName("com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks", false,
            proxy.getClass().getClassLoader()); }
        catch (ClassNotFoundException missing) { throw new IOException("Managed lifecycle requires the Canopy gateway", missing); }
        return new GatewaySessions(proxy, log, endpoints, directory, id + ":" + UUID.randomUUID(),
            config.getProperty("shared-secret", ""), audit);
    }

    private GatewaySessions(ProxyServer proxy, Logger log, Map<String, URI> endpoints,
                            String directory, String gateway, String secret, SessionAudit audit) {
        this.proxy = proxy; this.log = log; this.audit = audit; this.endpoints = Map.copyOf(endpoints);
        this.directory = endpoints.get(directory); this.gateway = gateway;
        client = new SessionControlClient(secret, audit);
        audit.event("gateway.ready", Map.of("gateway", gateway));
        timer.scheduleWithFixedDelay(() -> {
            if (stopped) return;
            async(() -> requestDirectory(new SessionCommand(SessionCommand.Operation.HEARTBEAT,
                null, null, gateway, null, null, null, 0))).exceptionally(error -> null);
            closing.forEach(this::finalizeLogout);
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Subscribe(order = PostOrder.LAST)
    public EventTask login(LoginEvent event) {
        if (!event.getResult().isAllowed()) return null;
        Player player = event.getPlayer();
        PlayerSession.GatewayToken token = new PlayerSession.GatewayToken(player.getUniqueId(), UUID.randomUUID(), gateway);
        audit.event("admission.request", Map.of("player", token.playerId(), "session", token.sessionId(), "gateway", gateway));
        return EventTask.resumeWhenComplete(retry(() -> open(token), Duration.ofSeconds(12))
            .thenAccept(state -> {
                audit.state("admission.opened", state);
                Connection local = new Connection(player, state);
                players.put(player.getUniqueId(), local);
                local.respawnHook = () -> respawn(local);
                hook(local, true);
                if (!player.isActive()) { local.departed = true; hook(local, false); closing.add(local); finalizeLogout(local); }
            }).exceptionally(error -> {
                event.setResult(ResultedEvent.ComponentResult.denied(Component.text("Player state is unavailable. Please try again.")));
                audit.failure("admission.denied", Map.of("player", token.playerId(), "session", token.sessionId()), error);
                log.warn("Managed admission could not finalize/open player state"); return null;
            }));
    }

    private PlayerSession open(PlayerSession.GatewayToken token) throws Exception {
        PlayerSession previous;
        try { previous = requestDirectory(SessionCommand.gateway(SessionCommand.Operation.FIND, token)); }
        catch (SessionCoordinator.Conflict unknown) { previous = null; }
        SessionCommand command = SessionCommand.gateway(SessionCommand.Operation.OPEN, token);
        if (previous != null && previous.phase() != PlayerSession.Phase.OFFLINE && previous.owner() != null) {
            try { return request(endpoint(previous.owner()), command); }
            catch (IOException unavailable) { /* directory may recover only after the owner lease expires */ }
        }
        return requestDirectory(command);
    }

    @Subscribe(order = PostOrder.LAST)
    public EventTask beforeConnect(ServerPreConnectEvent event) {
        Connection local = players.get(event.getPlayer().getUniqueId());
        if (local == null || !event.getResult().isAllowed()) return null;
        return EventTask.resumeWhenComplete(async(() -> {
            if (local.departed) throw new IllegalStateException("Session departed");
            PlayerSession state = requestDirectory(SessionCommand.gateway(SessionCommand.Operation.FIND, local.token));
            if (!state.gatewayToken().equals(local.token)) throw new SessionCoordinator.Conflict("Session replaced");
            RegisteredServer destination = event.getResult().getServer().orElseThrow();
            if (state.phase() == PlayerSession.Phase.CONNECTED) {
                if (state.snapshot().length > 0) {
                    String savedOwner = SessionSnapshot.decode(state.snapshot()).shard();
                    destination = proxy.getServer(savedOwner).orElseThrow(() -> new IOException("Saved owner unavailable"));
                }
                state = request(endpoint(destination.getServerInfo().getName()),
                    SessionCommand.gateway(SessionCommand.Operation.ATTACH, local.token));
                event.setResult(ServerPreConnectEvent.ServerResult.allowed(destination));
            } else if (state.phase() != PlayerSession.Phase.HANDOFF
                    || !destination.getServerInfo().getName().equals(state.target()) || !local.switching) {
                throw new SessionCoordinator.Conflict("Switch requires an acknowledged authority cut");
            }
            // Handoff login also requires a fully initialized managed destination.
            if (state.phase() == PlayerSession.Phase.HANDOFF) {
                PlayerSession confirmed = request(endpoint(destination.getServerInfo().getName()),
                    SessionCommand.gateway(SessionCommand.Operation.FIND, local.token));
                if (!confirmed.gatewayToken().equals(local.token) || confirmed.epoch() != state.epoch()
                        || confirmed.phase() != state.phase() || !Objects.equals(confirmed.transferId(), state.transferId()))
                    throw new SessionCoordinator.Conflict("Destination preparation was superseded");
            }
            local.state = state; audit.state("attachment.allowed", state); return state;
        }).exceptionally(error -> {
            event.setResult(ServerPreConnectEvent.ServerResult.denied());
            audit.failure("attachment.denied", SessionAudit.fields(local.state), error);
            log.warn("Managed backend connection held: authority or destination unavailable"); return null;
        }));
    }

    @Subscribe
    public void connected(ServerPostConnectEvent event) {
        Connection local = players.get(event.getPlayer().getUniqueId());
        if (local == null || local.departed) return;
        String owner = event.getPlayer().getCurrentServer().orElseThrow().getServerInfo().getName();
        PlayerSession state = local.state;
        SessionCommand command = state.phase() == PlayerSession.Phase.HANDOFF
            ? new SessionCommand(SessionCommand.Operation.COMMIT, local.token.playerId(), local.token.sessionId(), gateway,
                state.owner(), state.target(), state.transferId(), state.epoch())
            : SessionCommand.gateway(SessionCommand.Operation.ACTIVATE, local.token);
        retry(() -> request(endpoint(owner), command), Duration.ofSeconds(12)).thenAccept(active -> {
            audit.state("activation.completed", active);
            local.state = active; local.switching = false;
            if (local.departed) finalizeLogout(local);
            else if (local.respawning) handover(local, "sendRespawnRequest").whenComplete((ignored, error) -> local.respawning = false);
        }).exceptionally(error -> { audit.failure("activation.held", SessionAudit.fields(local.state), error); log.warn("Managed activation is held until backend state is ready"); return null; });
    }

    private void respawn(Connection local) {
        if (local.departed || local.switching || local.respawning) { audit.state("respawn.duplicate_held", local.state); return; }
        audit.state("respawn.request", local.state);
        local.respawning = true;
        String owner = local.player.getCurrentServer().orElseThrow().getServerInfo().getName();
        UUID operation = UUID.randomUUID();
        SessionCommand command = new SessionCommand(SessionCommand.Operation.ROUTE_RESPAWN, local.token.playerId(),
            local.token.sessionId(), gateway, owner, null, operation, 0);
        retry(() -> request(endpoint(owner), command), Duration.ofSeconds(12)).thenAccept(prepared -> {
            audit.state("handoff.prepared", prepared);
            local.state = prepared;
            if (local.departed) { finalizeLogout(local); return; }
            if (prepared.phase() == PlayerSession.Phase.ACTIVE) {
                handover(local, "sendRespawnRequest").whenComplete((ignored, error) -> local.respawning = false);
            } else {
                local.switching = true;
                handover(local, "beginRespawn").thenRun(() -> local.player.createConnectionRequest(
                    proxy.getServer(prepared.target()).orElseThrow()).connect().whenComplete((result, error) -> {
                        if (error != null || result == null || !result.isSuccessful()) {
                            local.respawning = false;
                            cancel(local, new SessionCommand(SessionCommand.Operation.CANCEL, local.token.playerId(),
                                local.token.sessionId(), gateway, prepared.owner(), prepared.target(), operation, prepared.epoch()));
                        }
                    })).exceptionally(error -> {
                        local.respawning = false;
                        cancel(local, new SessionCommand(SessionCommand.Operation.CANCEL, local.token.playerId(),
                            local.token.sessionId(), gateway, prepared.owner(), prepared.target(), operation, prepared.epoch()));
                        return null;
                    });
            }
        }).exceptionally(error -> { local.respawning = false; audit.failure("respawn.held", SessionAudit.fields(local.state), error); log.warn("Respawn is held until durable state and destination are available"); return null; });
    }

    private void hook(Connection local, boolean add) {
        try {
            Class<?> hooks = Class.forName("com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks", true,
                proxy.getClass().getClassLoader());
            hooks.getMethod(add ? "register" : "unregister", UUID.class, Runnable.class)
                .invoke(null, local.player.getUniqueId(), local.respawnHook);
        } catch (ReflectiveOperationException invalid) { throw new IllegalStateException("CSG lifecycle hooks unavailable", invalid); }
    }

    /** Fork integration stays on the connection loop; the published Velocity API has no raw-play input gate. */
    private CompletableFuture<Void> handover(Connection local, String method) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            Object connection = local.player.getClass().getMethod("getConnection").invoke(local.player);
            var loop = (java.util.concurrent.Executor) connection.getClass().getMethod("eventLoop").invoke(connection);
            loop.execute(() -> {
                try {
                    if (local.departed) throw new IllegalStateException("Player departed");
                    Object handler = connection.getClass().getMethod("getActiveSessionHandler").invoke(connection);
                    Object handover = handler.getClass().getMethod("getCanopyHandover").invoke(handler);
                    handover.getClass().getMethod(method).invoke(handover); audit.event("transport.release", Map.of("player", local.token.playerId(), "session", local.token.sessionId(), "operation", method)); done.complete(null);
                } catch (Exception failure) { audit.failure("transport.failed", SessionAudit.fields(local.state), failure); done.completeExceptionally(failure); }
            });
        } catch (Exception failure) { audit.failure("transport.failed", SessionAudit.fields(local.state), failure); done.completeExceptionally(failure); }
        return done;
    }

    /** Called after CSG has placed its cut marker in the source's ordered input stream. */
    public void switchTo(ServerConnection source, RegisteredServer destination) {
        Connection local = players.get(source.getPlayer().getUniqueId());
        if (local == null || local.departed || local.switching) return;
        local.switching = true;
        audit.state("handoff.request", local.state);
        UUID transfer = UUID.randomUUID();
        async(() -> {
            PlayerSession before = requestDirectory(SessionCommand.gateway(SessionCommand.Operation.FIND, local.token));
            if (!before.gatewayToken().equals(local.token) || !source.getServerInfo().getName().equals(before.owner()))
                throw new SessionCoordinator.Conflict("Source was superseded");
            local.state = before;
            SessionCommand prepare = new SessionCommand(SessionCommand.Operation.PREPARE, local.token.playerId(),
                local.token.sessionId(), gateway, source.getServerInfo().getName(), destination.getServerInfo().getName(),
                transfer, before.epoch());
            return request(endpoint(before.owner()), prepare);
        }).thenCompose(prepared -> {
            audit.state("handoff.prepared", prepared);
            local.state = prepared;
            if (local.departed) { finalizeLogout(local); return CompletableFuture.completedFuture(null); }
            return onPlayerEventLoop(local, () -> {
                if (local.departed) throw new IllegalStateException("Player departed before handover connect");
                local.player.createConnectionRequest(destination).connect().whenComplete((result, error) -> {
                    if (error != null || result == null || !result.isSuccessful()) cancel(local, new SessionCommand(
                        SessionCommand.Operation.CANCEL, local.token.playerId(), local.token.sessionId(), gateway,
                        prepared.owner(), prepared.target(), transfer, prepared.epoch()));
                });
            });
        }).exceptionally(error -> {
            audit.failure("handoff.prepare_failed", SessionAudit.fields(local.state), error);
            PlayerSession before = local.state;
            cancel(local, new SessionCommand(SessionCommand.Operation.CANCEL, local.token.playerId(),
                local.token.sessionId(), gateway, source.getServerInfo().getName(), destination.getServerInfo().getName(),
                transfer, before.epoch())); return null;
        });
    }

    /** Constructing a Velocity request reads connection state, so even that work belongs on its event loop. */
    private CompletableFuture<Void> onPlayerEventLoop(Connection local, Runnable action) {
        try {
            Object connection = local.player.getClass().getMethod("getConnection").invoke(local.player);
            Executor eventLoop = (Executor) connection.getClass().getMethod("eventLoop").invoke(connection);
            return dispatchOnEventLoop(eventLoop, action);
        } catch (ReflectiveOperationException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    static CompletableFuture<Void> dispatchOnEventLoop(Executor eventLoop, Runnable action) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            eventLoop.execute(() -> {
                try { action.run(); result.complete(null); }
                catch (Throwable failure) { result.completeExceptionally(failure); }
            });
        } catch (RejectedExecutionException failure) { result.completeExceptionally(failure); }
        return result;
    }

    private void cancel(Connection local, SessionCommand prepared) {
        audit.state("handoff.cancel_requested", local.state);
        SessionCommand cancel = new SessionCommand(SessionCommand.Operation.CANCEL, local.token.playerId(), local.token.sessionId(),
            gateway, prepared.owner(), prepared.target(), prepared.transferId(), prepared.epoch());
        retry(() -> request(endpoint(prepared.owner()), cancel), Duration.ofSeconds(12)).thenAccept(resumed -> {
            audit.state("handoff.cancelled", resumed);
            local.state = resumed; local.switching = false;
            if (local.departed) finalizeLogout(local);
            else handover(local, "cancel").exceptionally(error -> { audit.failure("handoff.cancel_release_failed", SessionAudit.fields(local.state), error); log.warn("CSG cancellation release failed"); return null; });
        }).exceptionally(error -> { audit.failure("handoff.cancel_held", SessionAudit.fields(local.state), error); log.warn("Managed handoff cancellation remains held"); return null; });
    }

    @Subscribe
    public void disconnected(DisconnectEvent event) {
        Connection local = players.get(event.getPlayer().getUniqueId());
        if (local == null || local.player != event.getPlayer()) return;
        audit.state("session.client_disconnected", local.state);
        local.departed = true; closing.add(local); finalizeLogout(local);
        hook(local, false);
    }

    private void finalizeLogout(Connection local) {
        synchronized (local) {
            if (local.finalizing) return;
            local.finalizing = true;
        }
        async(() -> {
            PlayerSession current = requestDirectory(SessionCommand.gateway(SessionCommand.Operation.FIND, local.token));
            if (!current.gatewayToken().equals(local.token)) return current;
            URI owner = current.owner() == null ? directory : endpoint(current.owner());
            try { return request(owner, SessionCommand.gateway(SessionCommand.Operation.DISCONNECT, local.token)); }
            catch (Exception unavailable) {
                return requestDirectory(SessionCommand.gateway(SessionCommand.Operation.RECOVER_LOGOUT, local.token));
            }
        }).whenComplete((state, error) -> {
            synchronized (local) { local.finalizing = false; }
            if (error != null) audit.failure("logout.retry", SessionAudit.fields(local.state), error);
            if (error == null && (state.phase() == PlayerSession.Phase.OFFLINE || !state.gatewayToken().equals(local.token))) {
                audit.state("logout.finalized", state);
                closing.remove(local); players.remove(local.player.getUniqueId(), local);
            }
        });
    }

    /** Directory operations are idempotent and shared by every configured managed endpoint. */
    private PlayerSession requestDirectory(SessionCommand command) throws Exception {
        try { return request(directory, command); }
        catch (IOException unavailable) {
            for (URI endpoint : endpoints.values()) {
                if (endpoint.equals(directory)) continue;
                audit.event("directory.failover", SessionAudit.fields(command));
                try { return request(endpoint, command); }
                catch (IOException alsoUnavailable) { unavailable.addSuppressed(alsoUnavailable); }
            }
            throw unavailable;
        }
    }

    private URI endpoint(String owner) throws IOException {
        URI uri = endpoints.get(owner);
        if (uri == null) throw new IOException("Backend has no independent control endpoint");
        return uri;
    }
    private PlayerSession request(URI uri, SessionCommand command) throws Exception { return client.request(uri, command); }
    @FunctionalInterface private interface Call<T> { T get() throws Exception; }
    private <T> CompletableFuture<T> async(Call<T> call) {
        try { return CompletableFuture.supplyAsync(() -> {
            try { return call.get(); } catch (Exception error) { throw new CompletionException(error); }
        }, io); } catch (RejectedExecutionException full) { audit.failure("gateway.queue_rejected", Map.of("queued", io.getQueue().size()), full); return CompletableFuture.failedFuture(full); }
    }
    private <T> CompletableFuture<T> retry(Call<T> call, Duration budget) {
        CompletableFuture<T> result = new CompletableFuture<>();
        attempt(call, result, System.nanoTime() + budget.toNanos()); return result;
    }
    private <T> void attempt(Call<T> call, CompletableFuture<T> result, long deadline) {
        if (stopped || result.isDone()) { result.completeExceptionally(new IOException("Gateway closing")); return; }
        async(call).whenComplete((value, error) -> {
            if (error == null) result.complete(value);
            else if (System.nanoTime() >= deadline) { audit.failure("control.retry_exhausted", Map.of(), error); result.completeExceptionally(error); }
            else try { timer.schedule(() -> attempt(call, result, deadline), 100, TimeUnit.MILLISECONDS); }
                catch (RejectedExecutionException closed) { result.completeExceptionally(closed); }
        });
    }

    public void auditCore(UUID player, Map<String, Object> fields) {
        Connection local = players.get(player);
        if (local != null) {
            // Preserve native event fields while adding the durable session correlation.
            Map<String, Object> combined = SessionAudit.fields(local.state); combined.putAll(fields); fields = combined;
        }
        String event = (String) fields.remove("event");
        fields.put("player", player); audit.event(event, fields);
    }

    @Override public void close() {
        audit.event("gateway.stopping", Map.of("gateway", gateway));
        stopped = true;
        players.values().forEach(local -> { local.departed = true; finalizeLogout(local); });
        timer.shutdownNow(); io.shutdown();
        try { io.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
