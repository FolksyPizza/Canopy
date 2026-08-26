package com.folypizza.canopy.migration;

import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.World;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class EntityStateSerializer {
    private static final Logger log = LoggerFactory.getLogger(EntityStateSerializer.class);
    private static final byte ENTITY_HEADER = 0x10;
    private static final int POSITION_SIZE = 16;
    private static final int ORIENTATION_SIZE = 8;

    public record SerializedEntity(
        int version,
        UUID entityId,
        String entityType,
        double x, double y, double z,
        float yaw, float pitch,
        List<byte[]> items
    ) {}

    public static byte[] serializeEntity(org.bukkit.entity.Entity entity) {
        var data = new SerializedEntity(
            1,
            entity.getUniqueId(),
            entity.getType().getKey().toString(),
            entity.getLocation().getX(),
            entity.getLocation().getY(),
            entity.getLocation().getZ(),
            entity.getLocation().getYaw(),
            entity.getLocation().getPitch(),
            List.of()
        );
        return serializeEntityData(data);
    }

    public static byte[] serializeEntityData(SerializedEntity data) {
        byte[][] itemBytes = new byte[data.items.size()][];
        int itemsBytes = 0;
        for (int i = 0; i < data.items.size(); i++) {
            itemBytes[i] = data.items().get(i);
            itemsBytes += 4 + itemBytes[i].length;
        }

        byte[] typeBytes = data.entityType().getBytes();
        int totalSize = 1 + 4 + 16 + 4 + typeBytes.length
            + POSITION_SIZE + ORIENTATION_SIZE + 4 + itemsBytes;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(ENTITY_HEADER);
        buf.putInt(data.version());
        buf.putLong(data.entityId().getMostSignificantBits());
        buf.putLong(data.entityId().getLeastSignificantBits());
        buf.putInt(typeBytes.length);
        buf.put(typeBytes);
        buf.putDouble(data.x());
        buf.putDouble(data.y());
        buf.putDouble(data.z());
        buf.putFloat(data.yaw());
        buf.putFloat(data.pitch());
        buf.putInt(data.items().size());
        for (byte[] b : itemBytes) {
            buf.putInt(b.length);
            buf.put(b);
        }
        buf.flip();
        return buf.array();
    }

    public static SerializedEntity deserializeEntity(byte[] data, World world, UUID entityUid) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        byte header = buf.get();
        if (header != ENTITY_HEADER) {
            throw new IllegalArgumentException("Invalid entity header");
        }
        int version = buf.getInt();
        long msb = buf.getLong();
        long lsb = buf.getLong();
        UUID entityId = entityUid != null ? entityUid : new UUID(msb, lsb);
        int typeLen = buf.getInt();
        byte[] typeBytes = new byte[typeLen];
        buf.get(typeBytes);
        String entityType = new String(typeBytes);
        double x = buf.getDouble();
        double y = buf.getDouble();
        double z = buf.getDouble();
        float yaw = buf.getFloat();
        float pitch = buf.getFloat();
        int itemCount = buf.getInt();
        List<byte[]> items = new ArrayList<>(Math.min(itemCount, 1000));
        for (int i = 0; i < itemCount; i++) {
            int len = buf.getInt();
            if (len > 65536) {
                log.warn("Item size {} truncated to 65535", len);
                len = 65535;
            }
            byte[] item = new byte[len];
            buf.get(item);
            items.add(item);
        }
        return new SerializedEntity(version, entityId, entityType, x, y, z, yaw, pitch, items);
    }

    public static byte[] serializeInventory(Inventory inventory) {
        int payloadSize = 0;
        for (int i = 0; i < inventory.getSize(); i++) {
            ItemStack stack = inventory.getItem(i);
            if (stack != null && !stack.isEmpty()) {
                payloadSize += 4 + stack.serialize().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            } else {
                payloadSize += 4;
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(4 + payloadSize);
        buf.putInt(inventory.getSize());
        for (int i = 0; i < inventory.getSize(); i++) {
            ItemStack stack = inventory.getItem(i);
            if (stack != null && !stack.isEmpty()) {
                byte[] bytes = stack.serialize().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                buf.putInt(bytes.length);
                buf.put(bytes);
            } else {
                buf.putInt(0);
            }
        }
        buf.flip();
        return buf.array();
    }

    public static List<ItemStack> deserializeInventory(byte[] data, String defaultMaterial) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int size = buf.getInt();
        List<ItemStack> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int len = buf.getInt();
            if (len == 0 || len > 65536) {
                result.add(new ItemStack(org.bukkit.Material.AIR));
                continue;
            }
            byte[] itemBytes = new byte[len];
            buf.get(itemBytes);
            String json = new String(itemBytes);
            try {
                ItemStack item = new ItemStack(org.bukkit.Material.AIR);
                result.add(item);
            } catch (Exception e) {
                log.warn("Failed to deserialize inventory item: {}", e.getMessage());
                result.add(new ItemStack(org.bukkit.Material.AIR));
            }
        }
        return result;
    }
}
