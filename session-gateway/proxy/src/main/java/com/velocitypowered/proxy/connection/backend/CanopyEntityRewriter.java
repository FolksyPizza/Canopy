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
 * Entity-id translation for the "no respawn" seamless server switch
 * ({@code -Dcanopy.mode=SEAMLESS}).
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
 * <p>Packet handling uses canonical 1.21.11 IDs translated through explicit version tables; it is inert on
 * unknown protocols. Every parse is wrapped so a malformed buffer is forwarded untouched rather
 * than corrupting the stream.</p>
 */
public final class CanopyEntityRewriter {

  private volatile com.velocitypowered.proxy.connection.client.CanopyProtocolProfile profile;
  private boolean enabled = true;

  // --- Canonical clientbound PLAY packet IDs (protocol 774), mapped to each wire protocol ---
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
  // Per-backend client state that a respawn-free switch would otherwise leave behind.
  private static final int CB_SET_OBJECTIVE = 0x68;          // name, action (0 add, 1 remove, 2 update)
  private static final int CB_SET_PLAYER_TEAM = 0x6b;        // name, mode (0 add, 1 remove, ...)
  private static final int CB_GAME_EVENT = 0x26;
  private static final int CB_SET_CHUNK_CACHE_RADIUS = 0x5d;
  private static final int CB_SET_SIMULATION_DISTANCE = 0x6d;
  private static final int CB_CLEAR_TITLES = 0x0e;
  private static final int GAME_EVENT_CHANGE_GAME_MODE = 3;
  // Respawn is encode-only in the proxy's registry, so a backend's world change arrives here as a raw packet.
  private static final int CB_RESPAWN = 0x50;                 // dimension type (varint), world name (string), ...

  // --- Canonical serverbound PLAY packet IDs (protocol 774), mapped to each wire protocol ---
  private static final int SB_QUERY_ENTITY_NBT = 0x18;       // transactionId then entityId
  private static final int SB_USE_ENTITY = 0x19;             // target leads
  private static final int SB_PICK_ITEM_FROM_ENTITY = 0x24;
  private static final int SB_ENTITY_ACTION = 0x29;          // player's own id leads
  private static final int SB_UPDATE_CMD_MINECART = 0x36;

  private boolean initialized;
  private int clientSelfId;
  private int backendSelfId;
  private final Set<Integer> tracked = new HashSet<>();
  private final Set<String> objectives = new java.util.LinkedHashSet<>();
  private final Set<String> teams = new java.util.LinkedHashSet<>();
  private final Set<Integer> selfEffects = new java.util.LinkedHashSet<>();
  // The client's current world: dimension type id and world name, from the first join and every respawn since.
  private int worldType = Integer.MIN_VALUE;
  private String worldName;

  public void noteWorld(int dimensionType, String name) {
    this.worldType = dimensionType;
    this.worldName = name;
  }

  /** A respawn-free switch keeps the client's world, so it is only correct into the same world and dimension type. */
  public boolean sameWorld(int dimensionType, String name) {
    return dimensionType == worldType && java.util.Objects.equals(name, worldName);
  }

  /** True when the no-respawn path may run for this player's protocol. */
  public boolean active(ProtocolVersion version) {
    return enabled && supports(version);
  }

  public void enabled(boolean enabled) { this.enabled = enabled; }
  public boolean enabled() { return enabled; }
  public Integer clientEntityId() { return initialized ? clientSelfId : null; }
  public Integer backendEntityId() { return initialized ? backendSelfId : null; }

  public boolean supports(ProtocolVersion version) {
    profile = com.velocitypowered.proxy.connection.client.CanopyProtocolProfile.find(version.getProtocol());
    return profile != null;
  }

  /** Records the entity id the client was first bound to (its own player entity). */
  public void initSelf(int entityId) {
    this.clientSelfId = entityId;
    this.backendSelfId = entityId;
    this.initialized = true;
    this.tracked.clear();
    this.objectives.clear();
    this.teams.clear();
    this.selfEffects.clear();
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
      int packetId = profile.clientbound(ProtocolUtils.readVarInt(buf));
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
        case CB_RESPAWN: {
          int type = ProtocolUtils.readVarInt(buf);
          noteWorld(type, ProtocolUtils.readString(buf));
          buf.readerIndex(start);
          return buf;
        }
        case CB_SET_OBJECTIVE: {
          String name = ProtocolUtils.readString(buf);
          byte action = buf.readByte();
          if (action == 0) {
            objectives.add(name);
          } else if (action == 1) {
            objectives.remove(name);
          }
          buf.readerIndex(start);
          return buf;
        }
        case CB_SET_PLAYER_TEAM: {
          String name = ProtocolUtils.readString(buf);
          byte mode = buf.readByte();
          if (mode == 0) {
            teams.add(name);
          } else if (mode == 1) {
            teams.remove(name);
          }
          buf.readerIndex(start);
          return buf;
        }
        case CB_ENTITY_EFFECT:
        case CB_REMOVE_ENTITY_EFFECT: {
          int id = ProtocolUtils.readVarInt(buf);
          int effect = ProtocolUtils.readVarInt(buf);
          if (f(id) == clientSelfId) {
            if (packetId == CB_ENTITY_EFFECT) {
              selfEffects.add(effect);
            } else {
              selfEffects.remove(effect);
            }
          }
          buf.readerIndex(afterId);
          return rewriteLeadingVarint(buf, start, afterId);
        }
        case CB_SET_PASSENGERS:
          return rewritePassengers(buf, start, afterId);
        case CB_COLLECT:
          return rewriteTwoVarints(buf, start, afterId);
        case CB_ENTITY_STATUS:
          return rewriteI32Fields(buf, start, afterId, 1);
        case CB_ATTACH_ENTITY:
          return rewriteI32Fields(buf, start, afterId, 2);
        case CB_DAMAGE_EVENT:
          return rewriteDamage(buf, start, afterId);
        case 0x72: // entity sound: registry holder, category, entity id
          if (ProtocolUtils.readVarInt(buf) == 0) {
            ProtocolUtils.readString(buf);
            if (buf.readBoolean()) { buf.skipBytes(4); }
          }
          ProtocolUtils.readVarInt(buf);
          return rewriteLeadingVarint(buf, start, afterId);
        // Every packet whose first field is a single varint entity id.
        case 0x85: // projectile power
        case 0x900: // 26.3 swing animation
        case CB_ANIMATION:
        case CB_BLOCK_BREAK_ANIMATION:
        case CB_SYNC_ENTITY_POSITION:
        case CB_HURT_ANIMATION:
        case CB_REL_ENTITY_MOVE:
        case CB_ENTITY_MOVE_LOOK:
        case CB_MOVE_MINECART:
        case CB_ENTITY_LOOK:
        case CB_DEATH_COMBAT_EVENT:
        case CB_ENTITY_HEAD_ROTATION:
        case CB_CAMERA:
        case CB_ENTITY_METADATA:
        case CB_ENTITY_VELOCITY:
        case CB_ENTITY_EQUIPMENT:
        case CB_ENTITY_TELEPORT:
        case CB_ENTITY_UPDATE_ATTRIBUTES:
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
      int packetId = profile.serverbound(ProtocolUtils.readVarInt(buf));
      int afterId = buf.readerIndex();

      switch (packetId) {
        case 0x900: // 26.x attack
        case SB_USE_ENTITY:
        case SB_PICK_ITEM_FROM_ENTITY:
        case SB_ENTITY_ACTION:
        case SB_UPDATE_CMD_MINECART:
          return rewriteLeadingVarint(buf, start, afterId);
        case 0x901: { // 26.x spectator action uses an optional entity id encoded as id + 1
          int fieldStart = buf.readerIndex();
          int encoded = ProtocolUtils.readVarInt(buf);
          int fieldEnd = buf.readerIndex();
          int mapped = encoded == 0 ? 0 : f(encoded - 1) + 1;
          if (mapped == encoded) { buf.readerIndex(start); return buf; }
          return spliceVarint(buf, start, fieldStart, fieldEnd, mapped);
        }
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

  private ByteBuf rewriteDamage(ByteBuf buf, int start, int afterId) {
    int victim = ProtocolUtils.readVarInt(buf);
    int type = ProtocolUtils.readVarInt(buf);
    int cause = ProtocolUtils.readVarInt(buf);
    int direct = ProtocolUtils.readVarInt(buf);
    int mappedVictim = f(victim);
    int mappedCause = cause == 0 ? 0 : f(cause - 1) + 1;
    int mappedDirect = direct == 0 ? 0 : f(direct - 1) + 1;
    int end = buf.readerIndex();
    if (victim == mappedVictim && cause == mappedCause && direct == mappedDirect) {
      buf.readerIndex(start);
      return buf;
    }
    ByteBuf out = buf.alloc().buffer();
    out.writeBytes(buf, start, afterId - start);
    ProtocolUtils.writeVarInt(out, mappedVictim);
    ProtocolUtils.writeVarInt(out, type);
    ProtocolUtils.writeVarInt(out, mappedCause);
    ProtocolUtils.writeVarInt(out, mappedDirect);
    out.writeBytes(buf, end, buf.writerIndex() - end);
    buf.readerIndex(start);
    return out;
  }

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
    if (count < 0 || count > buf.readableBytes()) { throw new IllegalArgumentException("passenger count"); }
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
    if (count < 0 || count > buf.readableBytes()) { throw new IllegalArgumentException("entity count"); }
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
  /**
   * Writes one raw packet per piece of client state the current backend created (scoreboard objectives, teams,
   * effects on the player) that removes it, then forgets it. Sent on a switch, before the new backend's own state.
   */
  public java.util.List<ByteBuf> drainStateResets(io.netty.buffer.ByteBufAllocator alloc) {
    java.util.List<ByteBuf> out = new java.util.ArrayList<>();
    for (String name : objectives) {
      ByteBuf b = alloc.buffer();
      ProtocolUtils.writeVarInt(b, profile.clientboundWire(CB_SET_OBJECTIVE));
      ProtocolUtils.writeString(b, name);
      b.writeByte(1);
      out.add(b);
    }
    for (String name : teams) {
      ByteBuf b = alloc.buffer();
      ProtocolUtils.writeVarInt(b, profile.clientboundWire(CB_SET_PLAYER_TEAM));
      ProtocolUtils.writeString(b, name);
      b.writeByte(1);
      out.add(b);
    }
    for (int effect : selfEffects) {
      ByteBuf b = alloc.buffer();
      ProtocolUtils.writeVarInt(b, profile.clientboundWire(CB_REMOVE_ENTITY_EFFECT));
      ProtocolUtils.writeVarInt(b, clientSelfId);
      ProtocolUtils.writeVarInt(b, effect);
      out.add(b);
    }
    objectives.clear();
    teams.clear();
    selfEffects.clear();
    return out;
  }

  /** What the skipped join packet would have set: game mode, view and simulation distance; titles cleared. */
  public java.util.List<ByteBuf> buildJoinState(io.netty.buffer.ByteBufAllocator alloc, int gameMode, int viewDistance,
      int simulationDistance) {
    java.util.List<ByteBuf> out = new java.util.ArrayList<>();
    ByteBuf mode = alloc.buffer();
    ProtocolUtils.writeVarInt(mode, profile.clientboundWire(CB_GAME_EVENT));
    mode.writeByte(GAME_EVENT_CHANGE_GAME_MODE);
    mode.writeFloat(gameMode);
    out.add(mode);
    ByteBuf view = alloc.buffer();
    ProtocolUtils.writeVarInt(view, profile.clientboundWire(CB_SET_CHUNK_CACHE_RADIUS));
    ProtocolUtils.writeVarInt(view, viewDistance);
    out.add(view);
    ByteBuf sim = alloc.buffer();
    ProtocolUtils.writeVarInt(sim, profile.clientboundWire(CB_SET_SIMULATION_DISTANCE));
    ProtocolUtils.writeVarInt(sim, simulationDistance);
    out.add(sim);
    ByteBuf titles = alloc.buffer();
    ProtocolUtils.writeVarInt(titles, profile.clientboundWire(CB_CLEAR_TITLES));
    titles.writeBoolean(true);
    out.add(titles);
    return out;
  }

  public ByteBuf buildEntityDestroy(ByteBuf out, int[] ids) {
    ProtocolUtils.writeVarInt(out, profile.clientboundWire(CB_ENTITY_DESTROY));
    ProtocolUtils.writeVarInt(out, ids.length);
    for (int id : ids) {
      ProtocolUtils.writeVarInt(out, id);
    }
    return out;
  }
}
