package com.folksypizza.canopy.velocity;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.PostOrder;
import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.event.player.ServerConnectedEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.Dependency;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal Velocity plugin that performs Canopy's seamless server switch.
 *
 * A backend Canopy shard sends a plugin message on the "canopy:switch" channel containing
 * the target server name; this plugin connects the sending player to that server using
 * Velocity's native connection request (config-phase switch, no login screen). This is
 * used instead of the fragile BungeeCord "Connect" compatibility path.
 */
@Plugin(id = "canopyswitch", name = "CanopySwitch", version = "1.0",
        description = "Canopy seamless shard handover", dependencies = {
            @Dependency(id = "viaversion", optional = true),
            @Dependency(id = "viabackwards", optional = true),
            @Dependency(id = "viarewind", optional = true)
        })
public class CanopySwitchPlugin {

    public static final MinecraftChannelIdentifier CHANNEL =
        MinecraftChannelIdentifier.create("canopy", "switch");
    /**
     * Exact handover: the destination shard reports that it has applied the handed-over state. The gateway consumes
     * it; registering it here makes the proxy announce the channel to backends, which only send plugin messages on
     * channels the connection has registered.
     */
    public static final MinecraftChannelIdentifier READY_CHANNEL =
        MinecraftChannelIdentifier.create("canopy", "ready");
    public static final MinecraftChannelIdentifier REPAIR_CHANNEL =
        MinecraftChannelIdentifier.create("canopy", "respawn-repair");

    private final ProxyServer server;
    private final Logger logger;
    private AutoCloseable viaProtocol;
    private GatewaySessions sessions;
    private com.folksypizza.canopy.session.SessionAudit audit = com.folksypizza.canopy.session.SessionAudit.NONE;
    private java.util.function.BiConsumer<UUID, java.util.Map<String, Object>> auditHook;
    private final java.nio.file.Path dataDirectory;
    private final java.util.Map<UUID, Long> switchStartedNanos = new ConcurrentHashMap<>();
    private String previousManagedSessionsProperty;

    @Inject
    public CanopySwitchPlugin(ProxyServer server, Logger logger, @DataDirectory java.nio.file.Path dataDirectory) {
        this.server = server;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onInit(ProxyInitializeEvent event) {
        server.getChannelRegistrar().register(CHANNEL, READY_CHANNEL, REPAIR_CHANNEL,
            MinecraftChannelIdentifier.create("canopy", "cancelled"));
        logger.info("CanopySwitch ready — listening on {}", CHANNEL.getId());
        try {
            var config = new java.util.Properties();
            var file = dataDirectory.resolve("session.properties");
            if (java.nio.file.Files.exists(file)) try (var input = java.nio.file.Files.newInputStream(file)) { config.load(input); }
            audit = com.folksypizza.canopy.session.SessionAudit.configured(dataDirectory, "gateway", config, logger::warn);
            sessions = GatewaySessions.load(server, logger, dataDirectory, audit);
            auditHook = (player, fields) -> {
                if (sessions != null) sessions.auditCore(player, fields);
                else { String name = (String) fields.remove("event"); fields.put("player", player); audit.event(name, fields); }
            };
            try {
                Class.forName("com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks", true, server.getClass().getClassLoader())
                    .getMethod("registerAudit", java.util.function.BiConsumer.class, boolean.class)
                    .invoke(null, auditHook, Boolean.parseBoolean(config.getProperty("audit.packet-trace", "false")));
            } catch (ClassNotFoundException stockVelocity) { logger.info("CSG native audit hooks unavailable on this proxy"); }
            if (sessions != null) { server.getEventManager().register(this, sessions); logger.info("Managed CSG lifecycle enabled"); }
            previousManagedSessionsProperty = System.getProperty("canopy.managed.sessions.enabled");
            System.setProperty("canopy.managed.sessions.enabled", Boolean.toString(sessions != null));
        } catch (Exception failure) {
            audit.failure("gateway.start_failed", java.util.Map.of(), failure); audit.close();
            throw new IllegalStateException("Managed CSG configuration could not initialize", failure);
        }
    }

    @Subscribe(order = PostOrder.LATE)
    public void onViaInit(ProxyInitializeEvent event) {
        String configured = System.getProperty("canopy.via.protocol");
        if (configured == null) return;
        if (server.getPluginManager().getPlugin("viaversion").isEmpty()) {
            throw new IllegalStateException("canopy.via.protocol requires ViaVersion on the gateway");
        }
        CanopyViaProtocol provider = new CanopyViaProtocol(Integer.parseInt(configured), logger);
        provider.install();
        viaProtocol = provider;
    }

    @Subscribe
    public void onShutdown(ProxyShutdownEvent event) throws Exception {
        try {
            if (sessions != null) sessions.close();
            if (viaProtocol != null) viaProtocol.close();
        } finally {
            if (auditHook != null) try {
                Class.forName("com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks", true, server.getClass().getClassLoader())
                    .getMethod("unregisterAudit", java.util.function.BiConsumer.class).invoke(null, auditHook);
            } catch (ReflectiveOperationException ignored) { }
            audit.close();
            if (previousManagedSessionsProperty == null) System.clearProperty("canopy.managed.sessions.enabled");
            else System.setProperty("canopy.managed.sessions.enabled", previousManagedSessionsProperty);
        }
    }

    @Subscribe
    public void onPluginMessage(PluginMessageEvent event) {
        if (!CHANNEL.equals(event.getIdentifier())) {
            return;
        }
        // Consume it — this is control traffic, not to be forwarded.
        event.setResult(PluginMessageEvent.ForwardResult.handled());
        if (!(event.getSource() instanceof ServerConnection source)) {
            return;
        }
        ByteArrayDataInput in = ByteStreams.newDataInput(event.getData());
        String target;
        try {
            target = in.readUTF();
        } catch (Exception e) {
            audit.failure("switch.invalid", java.util.Map.of(), e);
            logger.warn("Malformed canopy:switch message: {}", e.getMessage());
            return;
        }
        Player player = source.getPlayer();
        Optional<RegisteredServer> dest = server.getServer(target);
        if (dest.isEmpty()) {
            audit.event("switch.unknown_target", java.util.Map.of("player", player.getUniqueId(), "target", target));
            logger.warn("canopy:switch to unknown server '{}'", target);
            return;
        }
        if (source.getServerInfo().getName().equalsIgnoreCase(target)) {
            return; // already there
        }
        if (sessions != null) { sessions.switchTo(source, dest.get()); return; }
        audit.event("switch.request", java.util.Map.of("player", player.getUniqueId(), "target", target));
        long started = System.nanoTime();
        switchStartedNanos.put(player.getUniqueId(), started);
        logger.info("Switching {} -> {}", player.getUsername(), target);
        player.createConnectionRequest(dest.get()).connectWithIndication().whenComplete((success, error) -> {
            if (error != null || !Boolean.TRUE.equals(success)) {
                audit.event("switch.failed", java.util.Map.of("player", player.getUniqueId(), "target", target));
                Long began = switchStartedNanos.remove(player.getUniqueId());
                if (began != null) {
                    logger.warn("Switch {} -> {} failed after {} ms{}", player.getUsername(), target,
                        (System.nanoTime() - began) / 1_000_000L,
                        error == null ? "" : ": " + error.getMessage());
                }
            }
        });
    }

    @Subscribe
    public void onLogin(com.velocitypowered.api.event.connection.LoginEvent event) {
        audit.event("client.admission", java.util.Map.of("player", event.getPlayer().getUniqueId(),
            "protocol", event.getPlayer().getProtocolVersion().getProtocol(), "reason", event.getResult().isAllowed() ? "allowed" : "denied"));
    }

    @Subscribe
    public void onDisconnect(com.velocitypowered.api.event.connection.DisconnectEvent event) {
        audit.event("client.disconnected", java.util.Map.of("player", event.getPlayer().getUniqueId()));
        switchStartedNanos.remove(event.getPlayer().getUniqueId());
    }

    @Subscribe
    public void onServerConnected(ServerConnectedEvent event) {
        Long started = switchStartedNanos.remove(event.getPlayer().getUniqueId());
        if (started != null) {
            audit.event("switch.completed", java.util.Map.of("player", event.getPlayer().getUniqueId(), "target", event.getServer().getServerInfo().getName(), "durationMs", (System.nanoTime() - started) / 1_000_000L));
            logger.info("Switch to {} completed for {} in {} ms",
                event.getServer().getServerInfo().getName(), event.getPlayer().getUsername(),
                (System.nanoTime() - started) / 1_000_000L);
        }
    }
}
