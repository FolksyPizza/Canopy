package com.folypizza.canopy.migration;

import org.bukkit.GameMode;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Serializes and restores a player's cross-shard state: position, gamemode, flight,
 * vitals, XP, and full inventory (storage + armor + offhand + ender chest).
 *
 * Item stacks use Paper's {@code ItemStack.serializeAsBytes()} so NBT (enchants, custom
 * data) survives the hop. The payload is an opaque blob handed to the destination shard
 * over gRPC just before the proxy switch, then applied on join.
 */
public final class PlayerStateCodec {
    private static final Logger log = LoggerFactory.getLogger(PlayerStateCodec.class);
    private static final int VERSION = 1;

    private PlayerStateCodec() {}

    public static byte[] serialize(Player p) {
        return serialize(p, p.getLocation());
    }

    /** Serialize state but record {@code pos} as the destination position (deterministic landing). */
    public static byte[] serialize(Player p, Location pos) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(VERSION);

            out.writeDouble(pos.getX());
            out.writeDouble(pos.getY());
            out.writeDouble(pos.getZ());
            out.writeFloat(pos.getYaw());
            out.writeFloat(pos.getPitch());

            out.writeUTF(p.getGameMode().name());
            out.writeBoolean(p.getAllowFlight());
            out.writeBoolean(p.isFlying());
            out.writeDouble(p.getHealth());
            out.writeInt(p.getFoodLevel());
            out.writeFloat(p.getSaturation());
            out.writeFloat(p.getExp());
            out.writeInt(p.getLevel());

            PlayerInventory inv = p.getInventory();
            writeItems(out, inv.getStorageContents());
            writeItems(out, inv.getArmorContents());
            writeItem(out, inv.getItemInOffHand());
            writeItems(out, p.getEnderChest().getContents());

            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            log.warn("Failed to serialize player {}: {}", p.getName(), e.getMessage());
            return new byte[0];
        }
    }

    /** Apply a state blob to a freshly-joined player. Returns the target location (or null). */
    public static Location apply(Player p, byte[] blob, World world) {
        if (blob == null || blob.length == 0) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob));
            int version = in.readInt();
            if (version != VERSION) {
                log.warn("Unknown player-state version {}", version);
                return null;
            }
            double x = in.readDouble(), y = in.readDouble(), z = in.readDouble();
            float yaw = in.readFloat(), pitch = in.readFloat();
            Location loc = new Location(world, x, y, z, yaw, pitch);

            String gm = in.readUTF();
            boolean allowFlight = in.readBoolean();
            boolean flying = in.readBoolean();
            double health = in.readDouble();
            int food = in.readInt();
            float sat = in.readFloat();
            float exp = in.readFloat();
            int level = in.readInt();

            ItemStack[] storage = readItems(in);
            ItemStack[] armor = readItems(in);
            ItemStack offhand = readItem(in);
            ItemStack[] ender = readItems(in);

            PlayerInventory inv = p.getInventory();
            inv.setStorageContents(storage);
            inv.setArmorContents(armor);
            inv.setItemInOffHand(offhand);
            p.getEnderChest().setContents(ender);

            try { p.setGameMode(GameMode.valueOf(gm)); } catch (IllegalArgumentException ignored) {}
            p.setAllowFlight(allowFlight);
            p.setFlying(flying && allowFlight);
            try { if (health > 0) p.setHealth(Math.min(health, p.getMaxHealth())); } catch (Exception ignored) {}
            p.setFoodLevel(food);
            p.setSaturation(sat);
            p.setExp(exp);
            p.setLevel(level);

            return loc;
        } catch (Exception e) {
            log.warn("Failed to apply player state to {}: {}", p.getName(), e.getMessage());
            return null;
        }
    }

    private static void writeItems(DataOutputStream out, ItemStack[] items) throws Exception {
        out.writeInt(items == null ? 0 : items.length);
        if (items == null) return;
        for (ItemStack it : items) writeItem(out, it);
    }

    private static void writeItem(DataOutputStream out, ItemStack it) throws Exception {
        if (it == null || it.getType().isAir()) {
            out.writeInt(0);
            return;
        }
        byte[] b = it.serializeAsBytes();
        out.writeInt(b.length);
        out.write(b);
    }

    private static ItemStack[] readItems(DataInputStream in) throws Exception {
        int n = in.readInt();
        ItemStack[] arr = new ItemStack[n];
        for (int i = 0; i < n; i++) arr[i] = readItem(in);
        return arr;
    }

    private static ItemStack readItem(DataInputStream in) throws Exception {
        int len = in.readInt();
        if (len == 0) return null;
        byte[] b = new byte[len];
        in.readFully(b);
        return ItemStack.deserializeBytes(b);
    }
}
