package com.folypizza.canopy.routing;

import com.folypizza.canopy.registry.ShardRegistry;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

/**
 * {@code /region} — report the region a player is in and, for operators, move between
 * regions on demand.
 *
 * Usage:
 *   /region            show this shard, the seam boundary, and which side you are on
 *   /region list       list the shards this process knows about
 *   /region go         (op) hand yourself to the peer shard
 */
public class RegionCommand implements CommandExecutor {

    private final long shardId;
    private final double boundaryX;
    private final int buffer;
    private final boolean ownsWest;
    private final ShardRegistry registry;
    private final BoundaryTransferListener transfer;

    public RegionCommand(long shardId, double boundaryX, int buffer, boolean ownsWest,
                         ShardRegistry registry, BoundaryTransferListener transfer) {
        this.shardId = shardId;
        this.boundaryX = boundaryX;
        this.buffer = buffer;
        this.ownsWest = ownsWest;
        this.registry = registry;
        this.transfer = transfer;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        String sub = args.length > 0 ? args[0].toLowerCase() : "info";
        switch (sub) {
            case "list" -> {
                sender.sendMessage(Component.text("Known shards:", NamedTextColor.AQUA));
                sender.sendMessage(Component.text("  " + shardId + " (this shard, "
                    + (ownsWest ? "west" : "east") + ")", NamedTextColor.GRAY));
                for (ShardRegistry.ShardEntry s : registry.listShards()) {
                    if (s.shardId() == shardId) continue;
                    sender.sendMessage(Component.text("  " + s.shardId() + " @ " + s.address()
                        + (s.isHealthy() ? " (up)" : " (down)"), NamedTextColor.GRAY));
                }
                return true;
            }
            case "go" -> {
                if (!(sender instanceof Player p)) {
                    sender.sendMessage(Component.text("Only a player can move between regions.", NamedTextColor.RED));
                    return true;
                }
                if (!sender.isOp()) {
                    sender.sendMessage(Component.text("You do not have permission.", NamedTextColor.RED));
                    return true;
                }
                boolean ok = transfer.forceHandover(p);
                sender.sendMessage(ok
                    ? Component.text("Moving you to the peer region...", NamedTextColor.GREEN)
                    : Component.text("Cannot move right now (peer unreachable or already moving).", NamedTextColor.RED));
                return true;
            }
            default -> {
                String side = ownsWest ? "west" : "east";
                sender.sendMessage(Component.text("Region: shard " + shardId + " (" + side + " of x="
                    + (int) boundaryX + ", buffer " + buffer + ")", NamedTextColor.AQUA));
                if (sender instanceof Player p) {
                    double x = p.getLocation().getX();
                    sender.sendMessage(Component.text("You are at x=" + (int) x, NamedTextColor.GRAY));
                }
                sender.sendMessage(Component.text("Use /region list or /region go.", NamedTextColor.DARK_GRAY));
                return true;
            }
        }
    }
}
