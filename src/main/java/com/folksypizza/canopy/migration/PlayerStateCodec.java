package com.folksypizza.canopy.migration;

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
    private static final int VERSION = 5;

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
        return serialize(p, pos, motion, null);
    }

    public static byte[] serializeDeath(Player p, org.bukkit.event.entity.PlayerDeathEvent death) {
        return serialize(p, p.getLocation(), new LandingMotion(0, 0, 0), death);
    }

    private static byte[] serialize(Player p, Location pos, LandingMotion motion,
                                    org.bukkit.event.entity.PlayerDeathEvent death) {
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
            out.writeDouble(death == null ? p.getHealth() : 0);
            out.writeInt(p.getFoodLevel());
            out.writeFloat(p.getSaturation());
            out.writeFloat(death == null || death.getKeepLevel() ? p.getExp()
                : Math.min(1.0f, (float) death.getNewExp() / xpToNextLevel(death.getNewLevel())));
            out.writeInt(death == null || death.getKeepLevel() ? p.getLevel() : death.getNewLevel());
            out.writeInt(p.getInventory().getHeldItemSlot());   // selected hotbar slot (v3)

            PlayerInventory inv = p.getInventory();
            var keptItems = death == null ? null : new java.util.ArrayList<>(death.getItemsToKeep());
            writeItems(out, retained(inv.getStorageContents(), death, keptItems));
            writeItems(out, retained(inv.getArmorContents(), death, keptItems));
            writeItem(out, retained(new ItemStack[]{inv.getItemInOffHand()}, death, keptItems)[0]);
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

            out.writeInt(death == null || death.getKeepLevel() ? p.getTotalExperience() : death.getNewTotalExp());
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
            if (!supported(in.readInt())) return null;
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
            if (!supported(in.readInt())) return null;
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
        return apply(p, blob, world, arrival -> {
            if (p.isDead()) throw new IllegalStateException("Native respawn required before restoring alive state");
        });
    }

    /** Decode completely before repairing a stale death or mutating the player's current state. */
    public static Location apply(Player p, byte[] blob, World world,
                                 java.util.function.Consumer<Location> beforeRestore) {
        return apply(p, blob, world, beforeRestore, false);
    }

    public static Location apply(Player p, byte[] blob, World world,
                                 java.util.function.Consumer<Location> beforeRestore, boolean dead) {
        if (blob == null || blob.length == 0) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob));
            int version = in.readInt();
            if (!supported(version)) {
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

            int totalExperience = version >= 5 ? in.readInt() : totalXp(level, exp);
            if (totalExperience < 0) throw new IllegalArgumentException("Invalid total XP");
            if (!Double.isFinite(health) || (dead ? health != 0 : health <= 0) || !Double.isFinite(x) || !Double.isFinite(y)
                    || !Double.isFinite(z) || !Float.isFinite(yaw) || !Float.isFinite(pitch)
                    || food < 0 || food > 20 || !Float.isFinite(sat) || sat < 0
                    || !Float.isFinite(exp) || exp < 0 || exp > 1 || level < 0
                    || heldSlot < 0 || heldSlot > 8 || in.available() != 0) {
                throw new IllegalArgumentException("An alive, complete arrival snapshot is required");
            }
            GameMode.valueOf(gm);
            beforeRestore.accept(loc);

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
            if (!dead) p.setHealth(Math.min(health, p.getMaxHealth()));
            p.setFoodLevel(food);
            try { if (heldSlot >= 0 && heldSlot <= 8) inv.setHeldItemSlot(heldSlot); } catch (Exception ignored) {}
            p.setSaturation(sat);
            p.setExp(exp);
            p.setLevel(level);
            p.setTotalExperience(totalExperience);
            for (org.bukkit.potion.PotionEffect current : p.getActivePotionEffects()) {
                p.removePotionEffect(current.getType());
            }
            for (org.bukkit.potion.PotionEffect effect : effects) p.addPotionEffect(effect);

            // Flush current vitals before canopy:ready; the native next-tick update may otherwise
            // arrive after the gateway releases an old destination health/XP packet.
            if (dead && !p.isDead()) p.setHealth(0);
            p.sendHealthUpdate();
            p.sendExperienceChange(exp, level);

            return loc;
        } catch (Exception e) {
            log.warn("Failed to apply player state to {}: {}", p.getName(), e.getMessage());
            return null;
        }
    }

    private static boolean supported(int version) { return version == 4 || version == VERSION; }
    private static int xpToNextLevel(int level) {
        return (int) Math.min(Integer.MAX_VALUE, level >= 30 ? 112L + (level - 30L) * 9
            : level >= 15 ? 37L + (level - 15L) * 5 : 7L + level * 2L);
    }
    private static int totalXp(int level, float fraction) {
        double base = level <= 16 ? (double) level * level + 6.0 * level
            : level <= 31 ? 2.5 * level * level - 40.5 * level + 360
            : 4.5 * level * level - 162.5 * level + 2220;
        return (int) Math.min(Integer.MAX_VALUE, base + Math.round(fraction * xpToNextLevel(level)));
    }

    private static ItemStack[] retained(ItemStack[] slots, org.bukkit.event.entity.PlayerDeathEvent death, java.util.List<ItemStack> remaining) {
        if (death == null || death.getKeepInventory()) return slots;
        ItemStack[] kept = new ItemStack[slots.length];
        for (int i = 0; i < slots.length; i++) {
            if (slots[i] == null) continue;
            int at = remaining.indexOf(slots[i]);
            if (at >= 0) kept[i] = remaining.remove(at).clone();
        }
        return kept;
    }

    public static Location location(byte[] blob, World world) {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(blob));
            if (!supported(in.readInt())) return null;
            double x = in.readDouble(), y = in.readDouble(), z = in.readDouble();
            float yaw = in.readFloat(), pitch = in.readFloat();
            if (!Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(z)
                    || !Float.isFinite(yaw) || !Float.isFinite(pitch)) return null;
            return new Location(world, x, y, z, yaw, pitch);
        } catch (Exception invalid) { return null; }
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
        if (n < 0 || n > 128) throw new IllegalArgumentException("Invalid inventory length");
        ItemStack[] arr = new ItemStack[n];
        for (int i = 0; i < n; i++) arr[i] = readItem(in);
        return arr;
    }

    private static ItemStack readItem(DataInputStream in) throws Exception {
        int len = in.readInt();
        if (len == 0) return null;
        if (len < 0 || len > in.available()) throw new IllegalArgumentException("Invalid item length");
        byte[] b = new byte[len];
        in.readFully(b);
        return ItemStack.deserializeBytes(b);
    }
}
