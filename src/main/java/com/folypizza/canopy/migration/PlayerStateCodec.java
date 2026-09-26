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
    private static final int VERSION = 4;

    /** Movement observed on the source shard when the crossing began. */
    public record LandingMotion(double stepX, double stepZ, long startedAtMillis) {}

    private PlayerStateCodec() {}

    public static byte[] serialize(Player p) {
        return serialize(p, p.getLocation(), new LandingMotion(0, 0, 0));
    }

    /** Serialize state but record {@code pos} as the destination position (deterministic landing). */
    public static byte[] serialize(Player p, Location pos) {
        return serialize(p, pos, new LandingMotion(0, 0, 0));
    }

    public static byte[] serialize(Player p, Location pos, LandingMotion motion) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(VERSION);

            out.writeDouble(pos.getX());
            out.writeDouble(pos.getY());
            out.writeDouble(pos.getZ());
            out.writeFloat(pos.getYaw());
            out.writeFloat(pos.getPitch());

            org.bukkit.util.Vector vel = p.getVelocity();   // preserve momentum (v3)
            out.writeDouble(vel.getX());
            out.writeDouble(vel.getY());
            out.writeDouble(vel.getZ());

            out.writeDouble(motion.stepX());
            out.writeDouble(motion.stepZ());
            out.writeLong(motion.startedAtMillis());
            writeItem(out, activeFireworkBoost(p));

            out.writeUTF(p.getGameMode().name());
            out.writeBoolean(p.getAllowFlight());
            out.writeBoolean(p.isFlying());
            out.writeBoolean(p.isGliding());   // elytra state (v2)
            out.writeDouble(p.getHealth());
            out.writeInt(p.getFoodLevel());
            out.writeFloat(p.getSaturation());
            out.writeFloat(p.getExp());
            out.writeInt(p.getLevel());
            out.writeInt(p.getInventory().getHeldItemSlot());   // selected hotbar slot (v3)

            PlayerInventory inv = p.getInventory();
            writeItems(out, inv.getStorageContents());
            writeItems(out, inv.getArmorContents());
            writeItem(out, inv.getItemInOffHand());
            writeItems(out, p.getEnderChest().getContents());
            out.writeInt(p.getActivePotionEffects().size());
            for (org.bukkit.potion.PotionEffect effect : p.getActivePotionEffects()) {
                out.writeUTF(effect.getType().getKey().toString());
                out.writeInt(effect.getDuration());
                out.writeInt(effect.getAmplifier());
                out.writeBoolean(effect.isAmbient());
                out.writeBoolean(effect.hasParticles());
                out.writeBoolean(effect.hasIcon());
            }

            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            log.warn("Failed to serialize player {}: {}", p.getName(), e.getMessage());
            return new byte[0];
        }
    }

    /**
     * Reads just the stored velocity from a blob, to be applied after the arrival teleport (a
     * teleport resets velocity, so it cannot be set inside {@link #apply}). Returns null if absent.
     */
    public static org.bukkit.util.Vector readVelocity(byte[] blob) {
        if (blob == null || blob.length == 0) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob));
            if (in.readInt() < 3) return null;              // version
            in.readDouble(); in.readDouble(); in.readDouble(); // x, y, z
            in.readFloat(); in.readFloat();                    // yaw, pitch
            return new org.bukkit.util.Vector(in.readDouble(), in.readDouble(), in.readDouble());
        } catch (Exception e) {
            return null;
        }
    }

    public static LandingMotion readLandingMotion(byte[] blob) {
        if (blob == null || blob.length == 0) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob));
            if (in.readInt() != VERSION) return null;
            in.readDouble(); in.readDouble(); in.readDouble(); // position
            in.readFloat(); in.readFloat();                    // look
            in.readDouble(); in.readDouble(); in.readDouble(); // velocity
            return new LandingMotion(in.readDouble(), in.readDouble(), in.readLong());
        } catch (Exception e) {
            return null;
        }
    }

    /** Rocket attached to a gliding player, if one was active at the crossing. */
    public static ItemStack readFireworkBoost(byte[] blob) {
        if (blob == null || blob.length == 0) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob));
            if (in.readInt() != VERSION) return null;
            in.readDouble(); in.readDouble(); in.readDouble();
            in.readFloat(); in.readFloat();
            in.readDouble(); in.readDouble(); in.readDouble();
            in.readDouble(); in.readDouble(); in.readLong();
            return readItem(in);
        } catch (Exception e) {
            return null;
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

            in.readDouble(); in.readDouble(); in.readDouble(); // velocity — applied post-teleport, see readVelocity
            in.readDouble(); in.readDouble(); in.readLong(); // crossing movement and start time
            readItem(in); // attached firework boost, restored after arrival teleport

            String gm = in.readUTF();
            boolean allowFlight = in.readBoolean();
            boolean flying = in.readBoolean();
            boolean gliding = in.readBoolean();
            double health = in.readDouble();
            int food = in.readInt();
            float sat = in.readFloat();
            float exp = in.readFloat();
            int level = in.readInt();
            int heldSlot = in.readInt();

            ItemStack[] storage = readItems(in);
            ItemStack[] armor = readItems(in);
            ItemStack offhand = readItem(in);
            ItemStack[] ender = readItems(in);
            int effectCount = in.readInt();
            if (effectCount < 0 || effectCount > 128) throw new IllegalArgumentException("Invalid potion effect count");
            java.util.List<org.bukkit.potion.PotionEffect> effects = new java.util.ArrayList<>(effectCount);
            for (int i = 0; i < effectCount; i++) {
                org.bukkit.NamespacedKey key = org.bukkit.NamespacedKey.fromString(in.readUTF());
                int duration = in.readInt();
                int amplifier = in.readInt();
                boolean ambient = in.readBoolean();
                boolean particles = in.readBoolean();
                boolean icon = in.readBoolean();
                org.bukkit.potion.PotionEffectType type = key == null ? null
                    : org.bukkit.potion.PotionEffectType.getByKey(key);
                if (type != null) effects.add(new org.bukkit.potion.PotionEffect(
                    type, duration, amplifier, ambient, particles, icon));
            }

            PlayerInventory inv = p.getInventory();
            inv.setStorageContents(storage);
            inv.setArmorContents(armor);
            inv.setItemInOffHand(offhand);
            p.getEnderChest().setContents(ender);

            try { p.setGameMode(GameMode.valueOf(gm)); } catch (IllegalArgumentException ignored) {}
            p.setAllowFlight(allowFlight);
            p.setFlying(flying && allowFlight);
            // Reconcile the elytra pose so it doesn't stick after a no-respawn switch: the client
            // keeps its last pose across the swap, so we force the correct gliding state here.
            p.setGliding(gliding);
            try { if (health > 0) p.setHealth(Math.min(health, p.getMaxHealth())); } catch (Exception ignored) {}
            p.setFoodLevel(food);
            try { if (heldSlot >= 0 && heldSlot <= 8) inv.setHeldItemSlot(heldSlot); } catch (Exception ignored) {}
            p.setSaturation(sat);
            p.setExp(exp);
            p.setLevel(level);
            for (org.bukkit.potion.PotionEffect current : p.getActivePotionEffects()) {
                p.removePotionEffect(current.getType());
            }
            for (org.bukkit.potion.PotionEffect effect : effects) p.addPotionEffect(effect);

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

    private static ItemStack activeFireworkBoost(Player p) {
        if (!p.isGliding()) return null;
        try {
            for (org.bukkit.entity.Entity entity : p.getNearbyEntities(2, 2, 2)) {
                if (entity instanceof org.bukkit.entity.Firework firework
                    && firework.getBoostedEntity() == p) {
                    ItemStack rocket = new ItemStack(org.bukkit.Material.FIREWORK_ROCKET);
                    rocket.setItemMeta(firework.getFireworkMeta());
                    return rocket;
                }
            }
        } catch (Exception e) {
            log.debug("Could not capture active firework boost for {}: {}", p.getName(), e.getMessage());
        }
        return null;
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
