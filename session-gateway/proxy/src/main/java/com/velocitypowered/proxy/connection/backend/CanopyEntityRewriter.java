/*
 * Copyright (C) 2018-2023 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.connection.backend;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import java.util.HashSet;
import java.util.Set;

/**
 * Full entity-id translation for the "no respawn" seamless server switch
 * ({@code -Dcanopy.noRespawn=true}).
 *
 * <p>Velocity's normal fast switch sends a fresh JoinGame + Respawn on every backend change, which
 * resets the client's world (the "loading terrain" screen). The no-respawn path skips both, so the
 * client keeps its world — but it stays permanently locked to the entity id it received in its first
 * JoinGame, while each backend hands out entity ids independently. Left alone the new backend can
 * assign the client's locked id to some other entity, colliding with the player and freezing them.</p>
 *
 * <p>The fix is a bijection applied to <em>every</em> entity id in <em>every</em> packet, in both
 * directions:</p>
 *
 * <pre>
 *   f(id) = clientSelfId   if id == backendSelfId
 *           backendSelfId   if id == clientSelfId
 *           id              otherwise
 * </pre>
 *
 * <p>{@code f} is its own inverse, so the same function serves clientbound and serverbound traffic.
 * It moves the player's own entity onto the id the client is locked to, and relocates whatever
 * entity happened to occupy that id onto the now-free {@code backendSelfId} slot — so no collision
 * is possible. All other ids pass through untouched.</p>
 *
 * <p>Spawned entities are also tracked (by the id the client sees) so the previous backend's
 * entities can be despawned in one Remove-Entities packet at switch, since no Respawn clears them.</p>
 *
 * <p>Packet ids and layouts are those of protocol 774 (Minecraft 1.21.11); the rewriter is inert on
 * any other protocol. Every parse is wrapped so a malformed buffer is forwarded untouched rather
 * than corrupting the stream.</p>
 */
public final class CanopyEntityRewriter {

  private static final int SUPPORTED_PROTOCOL = 774;
  private static final boolean PROPERTY_ENABLED = Boolean.getBoolean("canopy.noRespawn");

  // --- Clientbound PLAY packet ids (protocol 774) ---
  private static final int CB_SPAWN_ENTITY = 0x01;
  private static final int CB_ANIMATION = 0x02;
  private static final int CB_BLOCK_BREAK_ANIMATION = 0x05;
  private static final int CB_DAMAGE_EVENT = 0x19;
  private static final int CB_ENTITY_STATUS = 0x22;          // i32
  private static final int CB_SYNC_ENTITY_POSITION = 0x23;
  private static final int CB_HURT_ANIMATION = 0x29;
  private static final int CB_REL_ENTITY_MOVE = 0x33;
  private static final int CB_ENTITY_MOVE_LOOK = 0x34;
  private static final int CB_MOVE_MINECART = 0x35;
  private static final int CB_ENTITY_LOOK = 0x36;
  private static final int CB_DEATH_COMBAT_EVENT = 0x42;
  private static final int CB_ENTITY_DESTROY = 0x4b;
  private static final int CB_REMOVE_ENTITY_EFFECT = 0x4c;
  private static final int CB_ENTITY_HEAD_ROTATION = 0x51;
  private static final int CB_CAMERA = 0x5b;
  private static final int CB_ENTITY_METADATA = 0x61;
  private static final int CB_ATTACH_ENTITY = 0x62;          // two i32
  private static final int CB_ENTITY_VELOCITY = 0x63;
  private static final int CB_ENTITY_EQUIPMENT = 0x64;
  private static final int CB_SET_PASSENGERS = 0x69;         // vehicle + array
  private static final int CB_COLLECT = 0x7a;                // two varints
  private static final int CB_ENTITY_TELEPORT = 0x7b;
  private static final int CB_ENTITY_UPDATE_ATTRIBUTES = 0x81;
  private static final int CB_ENTITY_EFFECT = 0x82;

  // --- Serverbound PLAY packet ids (protocol 774) ---
  private static final int SB_QUERY_ENTITY_NBT = 0x18;       // transactionId then entityId
  private static final int SB_USE_ENTITY = 0x19;             // target leads
  private static final int SB_PICK_ITEM_FROM_ENTITY = 0x24;
  private static final int SB_ENTITY_ACTION = 0x29;          // player's own id leads
  private static final int SB_UPDATE_CMD_MINECART = 0x36;

  private boolean initialized;
  private int clientSelfId;
  private int backendSelfId;
  private final Set<Integer> tracked = new HashSet<>();

  /** True when the no-respawn path may run for this player's protocol. */
  public boolean active(ProtocolVersion version) {
    return PROPERTY_ENABLED && version.getProtocol() == SUPPORTED_PROTOCOL;
  }

  /** Records the entity id the client was first bound to (its own player entity). */
  public void initSelf(int entityId) {
    this.clientSelfId = entityId;
    this.backendSelfId = entityId;
    this.initialized = true;
    this.tracked.clear();
  }

  public boolean isInitialized() {
    return initialized;
  }

  /** Points the bijection at the new backend after a seamless switch. */
  public void onSwitch(int newBackendSelfId) {
    this.backendSelfId = newBackendSelfId;
  }

  /** Returns every (client-side) entity id spawned by the current backend and clears the set. */
  public int[] drainTracked() {
    int[] out = new int[tracked.size()];
    int i = 0;
    for (int id : tracked) {
      out[i++] = id;
    }
    tracked.clear();
    return out;
  }

  /** The translation bijection (its own inverse). */
  private int f(int id) {
    if (id == backendSelfId) {
      return clientSelfId;
    }
    if (id == clientSelfId) {
      return backendSelfId;
    }
    return id;
  }

  // ---------------------------------------------------------------------------------------------
  // Clientbound (backend -> client)
  // ---------------------------------------------------------------------------------------------

  public ByteBuf processClientbound(ByteBuf buf) {
    int start = buf.readerIndex();
    try {
      int packetId = ProtocolUtils.readVarInt(buf);
      int afterId = buf.readerIndex();

      switch (packetId) {
        case CB_SPAWN_ENTITY: {
          // Translate the id and remember it (client-side) so it can be despawned on switch.
          int id = ProtocolUtils.readVarInt(buf);
          int end = buf.readerIndex();
          int nid = f(id);
          tracked.add(nid);
          if (nid == id) {
            buf.readerIndex(start);
            return buf;
          }
          return spliceVarint(buf, start, afterId, end, nid);
        }
        case CB_ENTITY_DESTROY:
          return rewriteDestroy(buf, start, afterId);
        case CB_SET_PASSENGERS:
          return rewritePassengers(buf, start, afterId);
        case CB_COLLECT:
          return rewriteTwoVarints(buf, start, afterId);
        case CB_ENTITY_STATUS:
          return rewriteI32Fields(buf, start, afterId, 1);
        case CB_ATTACH_ENTITY:
          return rewriteI32Fields(buf, start, afterId, 2);
        // Every packet whose first field is a single varint entity id.
        case CB_ANIMATION:
        case CB_BLOCK_BREAK_ANIMATION:
        case CB_DAMAGE_EVENT:
        case CB_SYNC_ENTITY_POSITION:
        case CB_HURT_ANIMATION:
        case CB_REL_ENTITY_MOVE:
        case CB_ENTITY_MOVE_LOOK:
        case CB_MOVE_MINECART:
        case CB_ENTITY_LOOK:
        case CB_DEATH_COMBAT_EVENT:
        case CB_REMOVE_ENTITY_EFFECT:
        case CB_ENTITY_HEAD_ROTATION:
        case CB_CAMERA:
        case CB_ENTITY_METADATA:
        case CB_ENTITY_VELOCITY:
        case CB_ENTITY_EQUIPMENT:
        case CB_ENTITY_TELEPORT:
        case CB_ENTITY_UPDATE_ATTRIBUTES:
        case CB_ENTITY_EFFECT:
          return rewriteLeadingVarint(buf, start, afterId);
        default:
          buf.readerIndex(start);
          return buf;
      }
    } catch (Exception ignored) {
      buf.readerIndex(start);
      return buf;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Serverbound (client -> backend)
  // ---------------------------------------------------------------------------------------------

  public ByteBuf processServerbound(ByteBuf buf) {
    int start = buf.readerIndex();
    try {
      int packetId = ProtocolUtils.readVarInt(buf);
      int afterId = buf.readerIndex();

      switch (packetId) {
        case SB_USE_ENTITY:
        case SB_PICK_ITEM_FROM_ENTITY:
        case SB_ENTITY_ACTION:
        case SB_UPDATE_CMD_MINECART:
          return rewriteLeadingVarint(buf, start, afterId);
        case SB_QUERY_ENTITY_NBT:
          return rewriteSecondVarint(buf, start, afterId);
        default:
          buf.readerIndex(start);
          return buf;
      }
    } catch (Exception ignored) {
      buf.readerIndex(start);
      return buf;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Field rewriters
  // ---------------------------------------------------------------------------------------------

  private ByteBuf rewriteLeadingVarint(ByteBuf buf, int start, int afterId) {
    int fieldStart = buf.readerIndex();
    int id = ProtocolUtils.readVarInt(buf);
    int fieldEnd = buf.readerIndex();
    int nid = f(id);
    if (nid == id) {
      buf.readerIndex(start);
      return buf;
    }
    return spliceVarint(buf, start, fieldStart, fieldEnd, nid);
  }

  /** Rewrites the second leading varint (the first is skipped, e.g. a transaction id). */
  private ByteBuf rewriteSecondVarint(ByteBuf buf, int start, int afterId) {
    ProtocolUtils.readVarInt(buf);            // skip first field
    int fieldStart = buf.readerIndex();
    int id = ProtocolUtils.readVarInt(buf);
    int fieldEnd = buf.readerIndex();
    int nid = f(id);
    if (nid == id) {
      buf.readerIndex(start);
      return buf;
    }
    return spliceVarint(buf, start, fieldStart, fieldEnd, nid);
  }

  private ByteBuf rewriteTwoVarints(ByteBuf buf, int start, int afterId) {
    int a = ProtocolUtils.readVarInt(buf);
    int b = ProtocolUtils.readVarInt(buf);
    int end = buf.readerIndex();
    int na = f(a);
    int nb = f(b);
    if (na == a && nb == b) {
      buf.readerIndex(start);
      return buf;
    }
    ByteBuf out = buf.alloc().buffer();
    out.writeBytes(buf, start, afterId - start);
    ProtocolUtils.writeVarInt(out, na);
    ProtocolUtils.writeVarInt(out, nb);
    out.writeBytes(buf, end, buf.writerIndex() - end);
    buf.readerIndex(start);
    return out;
  }

  private ByteBuf rewritePassengers(ByteBuf buf, int start, int afterId) {
    int vehicle = ProtocolUtils.readVarInt(buf);
    int count = ProtocolUtils.readVarInt(buf);
    int[] passengers = new int[count];
    boolean needsRewrite = f(vehicle) != vehicle;
    for (int i = 0; i < count; i++) {
      passengers[i] = ProtocolUtils.readVarInt(buf);
      if (f(passengers[i]) != passengers[i]) {
        needsRewrite = true;
      }
    }
    int end = buf.readerIndex();
    if (!needsRewrite) {
      buf.readerIndex(start);
      return buf;
    }
    ByteBuf out = buf.alloc().buffer();
    out.writeBytes(buf, start, afterId - start);
    ProtocolUtils.writeVarInt(out, f(vehicle));
    ProtocolUtils.writeVarInt(out, count);
    for (int id : passengers) {
      ProtocolUtils.writeVarInt(out, f(id));
    }
    out.writeBytes(buf, end, buf.writerIndex() - end);
    buf.readerIndex(start);
    return out;
  }

  private ByteBuf rewriteDestroy(ByteBuf buf, int start, int afterId) {
    int count = ProtocolUtils.readVarInt(buf);
    int[] ids = new int[count];
    boolean needsRewrite = false;
    for (int i = 0; i < count; i++) {
      ids[i] = ProtocolUtils.readVarInt(buf);
      int nid = f(ids[i]);
      tracked.remove(nid);
      if (nid != ids[i]) {
        needsRewrite = true;
      }
    }
    int end = buf.readerIndex();
    if (!needsRewrite) {
      buf.readerIndex(start);
      return buf;
    }
    ByteBuf out = buf.alloc().buffer();
    out.writeBytes(buf, start, afterId - start);
    ProtocolUtils.writeVarInt(out, count);
    for (int id : ids) {
      ProtocolUtils.writeVarInt(out, f(id));
    }
    out.writeBytes(buf, end, buf.writerIndex() - end);
    buf.readerIndex(start);
    return out;
  }

  private ByteBuf rewriteI32Fields(ByteBuf buf, int start, int afterId, int nFields) {
    boolean needsRewrite = false;
    for (int i = 0; i < nFields; i++) {
      if (f(buf.getInt(afterId + i * 4)) != buf.getInt(afterId + i * 4)) {
        needsRewrite = true;
        break;
      }
    }
    if (!needsRewrite) {
      buf.readerIndex(start);
      return buf;
    }
    ByteBuf out = buf.alloc().buffer(buf.readableBytes());
    out.writeBytes(buf, start, afterId - start);
    for (int i = 0; i < nFields; i++) {
      out.writeInt(f(buf.getInt(afterId + i * 4)));
    }
    int rest = afterId + nFields * 4;
    out.writeBytes(buf, rest, buf.writerIndex() - rest);
    buf.readerIndex(start);
    return out;
  }

  /** Rebuilds {@code buf} with the varint in {@code [fieldStart, fieldEnd)} replaced by {@code newVal}. */
  private ByteBuf spliceVarint(ByteBuf buf, int start, int fieldStart, int fieldEnd, int newVal) {
    ByteBuf out = buf.alloc().buffer();
    out.writeBytes(buf, start, fieldStart - start);
    ProtocolUtils.writeVarInt(out, newVal);
    out.writeBytes(buf, fieldEnd, buf.writerIndex() - fieldEnd);
    buf.readerIndex(start);
    return out;
  }

  /**
   * Builds a raw Remove-Entities (0x4b) payload for the given client-side ids, ready to write
   * straight to the client connection (no frame-length prefix — the outbound pipeline adds it).
   */
  public ByteBuf buildEntityDestroy(ByteBuf out, int[] ids) {
    ProtocolUtils.writeVarInt(out, CB_ENTITY_DESTROY);
    ProtocolUtils.writeVarInt(out, ids.length);
    for (int id : ids) {
      ProtocolUtils.writeVarInt(out, id);
    }
    return out;
  }
}
