package com.folypizza.canopy.velocity;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.player.ServerConnectedEvent;
import com.velocitypowered.api.plugin.Plugin;
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
        description = "Canopy seamless shard handover")
public class CanopySwitchPlugin {

    public static final MinecraftChannelIdentifier CHANNEL =
        MinecraftChannelIdentifier.create("canopy", "switch");

    private final ProxyServer server;
    private final Logger logger;
    private final java.util.Map<UUID, Long> switchStartedNanos = new ConcurrentHashMap<>();

    @Inject
    public CanopySwitchPlugin(ProxyServer server, Logger logger) {
        this.server = server;
        this.logger = logger;
    }

    @Subscribe
    public void onInit(ProxyInitializeEvent event) {
        server.getChannelRegistrar().register(CHANNEL);
        logger.info("CanopySwitch ready — listening on {}", CHANNEL.getId());
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
            logger.warn("Malformed canopy:switch message: {}", e.getMessage());
            return;
        }
        Player player = source.getPlayer();
        Optional<RegisteredServer> dest = server.getServer(target);
        if (dest.isEmpty()) {
            logger.warn("canopy:switch to unknown server '{}'", target);
            return;
        }
        if (source.getServerInfo().getName().equalsIgnoreCase(target)) {
            return; // already there
        }
        long started = System.nanoTime();
        switchStartedNanos.put(player.getUniqueId(), started);
        logger.info("Switching {} -> {}", player.getUsername(), target);
        player.createConnectionRequest(dest.get()).connectWithIndication().whenComplete((success, error) -> {
            if (error != null || !Boolean.TRUE.equals(success)) {
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
    public void onServerConnected(ServerConnectedEvent event) {
        Long started = switchStartedNanos.remove(event.getPlayer().getUniqueId());
        if (started != null) {
            logger.info("Switch to {} completed for {} in {} ms",
                event.getServer().getServerInfo().getName(), event.getPlayer().getUsername(),
                (System.nanoTime() - started) / 1_000_000L);
        }
    }
}
