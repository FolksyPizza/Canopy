package com.folksypizza.canopy.velocity;

import com.velocitypowered.api.command.SimpleCommand;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.proxy.ConsoleCommandSource;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;

/** Operator bridge to the Canopy proxy's pre-dispatch maintenance input gate. */
final class CanopyFreezeCommand implements SimpleCommand {
    private static final String CORE_CLASS =
        "com.velocitypowered.proxy.connection.client.CanopyGameplayFreeze";
    private final ProxyServer server;

    CanopyFreezeCommand(ProxyServer server) {
        this.server = server;
    }

    @Override
    public boolean hasPermission(Invocation invocation) {
        // Always intercept this private control root before checking the Canopy operator permission.
        return true;
    }

    @Override
    public void execute(Invocation invocation) {
        if (!(invocation.source() instanceof ConsoleCommandSource)
            && !invocation.source().hasPermission("canopy.freeze")) {
            reply(invocation.source(), "Permission denied.");
            return;
        }
        String[] args = invocation.arguments();
        if (args.length != 2) {
            reply(invocation.source(), "Usage: /canopyfreeze <player|all> <on|off|status>");
            return;
        }
        Collection<Player> targets = resolve(args[0]);
        if (targets.isEmpty()) {
            reply(invocation.source(), "No connected players matched " + args[0] + ".");
            return;
        }
        String action = args[1].toLowerCase(Locale.ROOT);
        try {
            Class<?> core = Class.forName(CORE_CLASS, true, server.getClass().getClassLoader());
            if (action.equals("status")) {
                Method status = core.getMethod("status", Collection.class);
                reply(invocation.source(), String.valueOf(status.invoke(null, targets)));
                return;
            }
            if (!action.equals("on") && !action.equals("off")) {
                reply(invocation.source(), "Expected on, off, or status.");
                return;
            }
            Method setFrozen = core.getMethod("setFrozen", Collection.class, boolean.class);
            Object outcome = setFrozen.invoke(null, targets, action.equals("on"));
            if (!(outcome instanceof CompletableFuture<?> future)) {
                reply(invocation.source(), "Freeze control returned no completion result.");
                return;
            }
            CommandSource source = invocation.source();
            future.whenComplete((message, failure) -> {
                if (failure != null) {
                    reply(source, "Freeze control failed: " + safeMessage(failure));
                } else {
                    reply(source, String.valueOf(message));
                }
            });
        } catch (ReflectiveOperationException | LinkageError failure) {
            reply(invocation.source(), "Canopy gameplay freeze is unavailable on this proxy.");
        }
    }

    @Override
    public List<String> suggest(Invocation invocation) {
        if (!(invocation.source() instanceof ConsoleCommandSource)
            && !invocation.source().hasPermission("canopy.freeze")) {
            return List.of();
        }
        String[] args = invocation.arguments();
        if (args.length <= 1) {
            List<String> names = new ArrayList<>();
            names.add("all");
            server.getAllPlayers().forEach(player -> names.add(player.getUsername()));
            return names;
        }
        if (args.length == 2) {
            return List.of("on", "off", "status");
        }
        return List.of();
    }

    private Collection<Player> resolve(String target) {
        if (target.equalsIgnoreCase("all")) return new ArrayList<>(server.getAllPlayers());
        return server.getPlayer(target).<Collection<Player>>map(List::of).orElseGet(List::of);
    }

    private static void reply(CommandSource source, String message) {
        source.sendMessage(Component.text("[Canopy freeze] " + message));
    }

    private static String safeMessage(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }
}
