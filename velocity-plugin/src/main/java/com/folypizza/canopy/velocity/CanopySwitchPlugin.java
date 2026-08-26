package com.folypizza.canopy.velocity;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import org.slf4j.Logger;

import java.util.Optional;

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
        logger.info("Switching {} -> {}", player.getUsername(), target);
        player.createConnectionRequest(dest.get()).fireAndForget();
    }
}
