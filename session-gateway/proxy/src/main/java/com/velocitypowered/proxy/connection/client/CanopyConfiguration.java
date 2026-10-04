package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.protocol.MinecraftPacket;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.packet.config.RegistrySyncPacket;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;

/** Measures configuration received on this attachment, instead of trusting deployment assertions alone. */
public final class CanopyConfiguration {
  private final MessageDigest digest;
  private int registries;
  private int tags;
  private boolean invalid;
  private final Set<String> knownPacksPayloads = new HashSet<>();

  public CanopyConfiguration() {
    try { digest = MessageDigest.getInstance("SHA-256"); }
    catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }

  public void record(MinecraftPacket packet, ProtocolVersion version) {
    String name = packet.getClass().getSimpleName();
    if (!(name.equals("RegistrySyncPacket") || name.equals("TagsUpdatePacket")
        || name.equals("KnownPacksPacket") || name.equals("ResourcePackRequestPacket")
        || name.equals("RemoveResourcePackPacket") || name.contains("Features"))) return;
    if (name.equals("RegistrySyncPacket")) registries++;
    if (name.equals("TagsUpdatePacket")) tags++;
    var buffer = Unpooled.buffer();
    try {
      if (packet instanceof RegistrySyncPacket registry) {
        buffer.writeBytes(registry.content(), registry.content().readerIndex(), registry.content().readableBytes());
      } else {
        packet.encode(buffer, ProtocolUtils.Direction.CLIENTBOUND, version);
      }
      if (name.equals("RegistrySyncPacket") && version.compareTo(ProtocolVersion.MINECRAFT_1_20_5) >= 0) canonicalRegistry(buffer, version);
      if (name.equals("TagsUpdatePacket")) canonicalTags(buffer);
      byte[] bytes = new byte[buffer.readableBytes()]; buffer.getBytes(buffer.readerIndex(), bytes);
      // A repeated identical offer is handshake noise; keep distinct offers in the fingerprint.
      if (name.equals("KnownPacksPacket") && !knownPacksPayloads.add(Base64.getEncoder().encodeToString(bytes))) return;
      byte[] label = name.getBytes(StandardCharsets.UTF_8);
      digest.update((byte) label.length); digest.update(label);
      digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
    } catch (RuntimeException malformed) { invalid = true; } finally { buffer.release(); }
  }

  /** Tag memberships are sets; registry entry order remains untouched because it assigns numeric IDs. */
  private static void canonicalTags(io.netty.buffer.ByteBuf buffer) {
    java.util.Map<String, java.util.Map<String, int[]>> groups = new java.util.TreeMap<>();
    int size = ProtocolUtils.readVarInt(buffer);
    for (int i = 0; i < size; i++) {
      String registry = ProtocolUtils.readString(buffer);
      int count = ProtocolUtils.readVarInt(buffer);
      java.util.Map<String, int[]> tags = new java.util.TreeMap<>();
      for (int j = 0; j < count; j++) {
        String tag = ProtocolUtils.readString(buffer);
        int[] entries = ProtocolUtils.readVarIntArray(buffer);
        java.util.Arrays.sort(entries);
        tags.put(tag, entries);
      }
      groups.put(registry, tags);
    }
    buffer.clear();
    ProtocolUtils.writeVarInt(buffer, groups.size());
    groups.forEach((registry, tags) -> {
      ProtocolUtils.writeString(buffer, registry);
      ProtocolUtils.writeVarInt(buffer, tags.size());
      tags.forEach((tag, entries) -> {
        ProtocolUtils.writeString(buffer, tag);
        ProtocolUtils.writeVarIntArray(buffer, entries);
      });
    });
  }

  /** Registry IDs follow entry order. NBT compound keys are unordered; list and array order matters. */
  private static void canonicalRegistry(io.netty.buffer.ByteBuf input, ProtocolVersion version) {
    var output = Unpooled.buffer();
    try {
      ProtocolUtils.writeString(output, ProtocolUtils.readString(input));
      int entries = ProtocolUtils.readVarInt(input); ProtocolUtils.writeVarInt(output, entries);
      if (entries < 0 || entries > 1000000) throw new IllegalArgumentException("registry entry count");
      for (int i = 0; i < entries; i++) {
        ProtocolUtils.writeString(output, ProtocolUtils.readString(input));
        boolean present = input.readBoolean(); output.writeBoolean(present);
        if (present) canonicalTag(output, ProtocolUtils.readBinaryTag(input, version, net.kyori.adventure.nbt.BinaryTagIO.reader()), version);
      }
      if (input.isReadable()) throw new IllegalArgumentException("registry trailing bytes");
      input.clear(); input.writeBytes(output);
    } finally { output.release(); }
  }

  private static void canonicalTag(io.netty.buffer.ByteBuf output, net.kyori.adventure.nbt.BinaryTag tag, ProtocolVersion version) {
    if (tag instanceof net.kyori.adventure.nbt.CompoundBinaryTag compound) {
      output.writeByte(tag.type().id()); ProtocolUtils.writeVarInt(output, compound.size());
      for (String key : new java.util.TreeSet<>(compound.keySet())) {
        ProtocolUtils.writeString(output,key); canonicalTag(output,compound.get(key),version);
      }
    } else if (tag instanceof net.kyori.adventure.nbt.ListBinaryTag list) {
      output.writeByte(tag.type().id()); output.writeByte(list.elementType().id()); ProtocolUtils.writeVarInt(output,list.size());
      for (var value : list) canonicalTag(output,value,version);
    } else {
      ProtocolUtils.writeBinaryTag(output,version,tag);
    }
  }

  public String finish() {
    if (invalid || registries == 0 || tags == 0) return "";
    return HexFormat.of().formatHex(digest.digest());
  }
}
