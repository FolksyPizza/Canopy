/* Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later */
package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.MinecraftPacket;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Authenticated native chat-state migration. All calls are on the player's event loop. */
public final class CanopySignedChatHandoff {
  private final ConnectedPlayer player;
  private final ArrayDeque<Object> input = new ArrayDeque<>();
  private final ArrayDeque<ByteBuf> sourceChat = new ArrayDeque<>();
  private VelocityServerConnection source, destination;
  private ChatStateFrame binding;
  private ChatStateFrame.Assembly assembly;
  private byte[] key, snapshot;
  private CompletableFuture<Void> captured, restored;
  private int bytes;
  private boolean active, sourceCut;

  public CanopySignedChatHandoff(ConnectedPlayer player) { this.player = player; }

  public boolean active() { return active; }
  public boolean restored() { return restored != null && restored.isDone() && !restored.isCompletedExceptionally(); }

  public boolean canCommit(VelocityServerConnection target) {
    return active && restored() && target == destination && target.isActive()
        && source != null && source.isActive() && player.getConnectedServer() == source;
  }

  public boolean required() { return Boolean.getBoolean("canopy.chatHandoff.enabled"); }

  public CompletableFuture<Void> capture(VelocityServerConnection target) {
    if (active) return CompletableFuture.failedFuture(new IllegalStateException("Chat handoff already active"));
    try {
      String file = System.getProperty("canopy.chatHandoff.keyFile", "");
      if (file.isEmpty()) throw new IllegalStateException("Missing chat handoff key file");
      key = Files.readAllBytes(Path.of(file));
      if (key.length < 32 || key.length > 4096) throw new IllegalStateException("Invalid chat handoff key");
      source = player.getConnectedServer();
      if (source == null || source == target || !source.isActive()) throw new IllegalStateException("Missing chat source");
      destination = target;
      binding = new ChatStateFrame(ChatStateFrame.CAPTURE, UUID.randomUUID(), player.getUniqueId(),
          source.getServerInfo().getName(), target.getServerInfo().getName(),
          System.currentTimeMillis() + 30000, 0, 1, 0, new byte[0]);
      active = true;
      audit("chat.capture", null);
      captured = new CompletableFuture<>();
      UUID expiryAttempt = binding.attempt();
      player.getConnection().eventLoop().schedule(() -> {
        if (active && binding.attempt().equals(expiryAttempt)) abort(new IllegalStateException("Chat handoff timed out"));
      }, 30, TimeUnit.SECONDS);
      player.getChatQueue().drain().whenCompleteAsync((unused, failure) ->
          drainCompleted(expiryAttempt, failure), player.getConnection().eventLoop());
      return captured;
    } catch (Exception failure) { clear(); return CompletableFuture.failedFuture(failure); }
  }

  boolean currentAttempt(UUID attempt) {
    return active && binding != null && binding.attempt().equals(attempt);
  }

  public UUID attemptId(VelocityServerConnection target) {
    return active && target == destination && binding != null ? binding.attempt() : null;
  }

  public boolean ownsAttempt(UUID attempt, VelocityServerConnection target) {
    return attempt != null && currentAttempt(attempt) && target == destination;
  }

  public void abortAttempt(UUID attempt, VelocityServerConnection target, Throwable failure) {
    if (ownsAttempt(attempt, target)) abort(failure);
  }

  /** Distinguishes an obsolete callback from a failed detach/join owned by this transition. */
  public boolean shouldRemediateFailure(UUID attempt, VelocityServerConnection target,
      VelocityServerConnection originalSource, boolean finishStarted, boolean finishCommitted) {
    if (finishCommitted) return false;
    if (!finishStarted) return ownsAttempt(attempt, target);
    if (active && !ownsAttempt(attempt, target)) return false;
    VelocityServerConnection inFlight = player.getConnectionInFlight();
    if (inFlight != null && inFlight != target) return false;
    VelocityServerConnection current = player.getConnectedServer();
    return current == null || current == originalSource || current == target;
  }

  void drainCompleted(UUID attempt, Throwable failure) {
    if (!currentAttempt(attempt)) return;
    if (failure != null) abort(failure);
    else send(source, ChatStateFrame.CAPTURE, new byte[0]);
  }

  /** The native source remains attached while asynchronous connection listeners finish. */
  public CompletableFuture<Void> finishAfter(CompletableFuture<?> connectedEvent,
      VelocityServerConnection target, java.util.concurrent.Executor eventLoop, Runnable finish) {
    return finishAfter(attemptId(target), connectedEvent, target, eventLoop, finish);
  }

  public CompletableFuture<Void> finishAfter(UUID attempt, CompletableFuture<?> connectedEvent,
      VelocityServerConnection target, java.util.concurrent.Executor eventLoop, Runnable finish) {
    return connectedEvent.thenRunAsync(() -> {
      if (!ownsAttempt(attempt, target) || !canCommit(target))
        throw new IllegalStateException("Chat handoff expired before source detach");
      finish.run();
    }, eventLoop);
  }

  public CompletableFuture<Void> restore(VelocityServerConnection target) {
    if (!active || target != destination || snapshot == null) return CompletableFuture.failedFuture(
        new IllegalStateException("Missing chat snapshot"));
    restored = new CompletableFuture<>();
    CompletableFuture<Void> result = restored;
    audit("chat.restore", null);
    send(destination, ChatStateFrame.RESTORE, snapshot);
    return result;
  }

  /** Reserved messages are consumed even if disabled, unsolicited, malformed or foreign. */
  public boolean receive(VelocityServerConnection sender, PluginMessagePacket packet) {
    if (!ChatStateFrame.CHANNEL.equals(packet.getChannel())) return false;
    if (!active || (sender != source && sender != destination)) return true;
    try {
      byte[] data = new byte[packet.content().readableBytes()];
      packet.content().getBytes(packet.content().readerIndex(), data);
      ChatStateFrame frame = ChatStateFrame.decode(data, key, System.currentTimeMillis());
      if (!frame.attempt().equals(binding.attempt()) || !frame.player().equals(binding.player())
          || !frame.source().equals(binding.source()) || !frame.destination().equals(binding.destination())
          || frame.expiresAt() != binding.expiresAt()) return true;
      if (frame.operation() == ChatStateFrame.REJECT && (sender == source || sender == destination)) {
        abort(new IllegalStateException("Backend rejected chat migration"));
      } else if (frame.operation() == ChatStateFrame.SNAPSHOT && sender == source && !sourceCut) {
        if (assembly == null) assembly = new ChatStateFrame.Assembly(frame);
        byte[] complete = assembly.add(frame);
        if (complete != null) { snapshot = complete; sourceCut = true; captured.complete(null); }
      } else if (frame.operation() == ChatStateFrame.RESTORED && sender == destination
          && restored != null && frame.totalLength() == 0) {
        restored.complete(null);
      }
    } catch (Exception failure) { abort(failure); }
    return true;
  }

  public boolean bufferRawChatInput(ByteBuf packet) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    if (profile == null || profile.serverbound(ProtocolUtils.readVarInt(packet.duplicate())) != 0x09) return false;
    return bufferInput(packet);
  }

  public boolean bufferInput(Object packet) {
    if (!active) return false;
    int size;
    try { size = packetSize(packet, ProtocolUtils.Direction.SERVERBOUND,
        player.getProtocolVersion(), ChatStateFrame.MAX_TOTAL); }
    catch (RuntimeException encodingFailure) { abort(encodingFailure); return false; }
    if (input.size() >= 256 || bytes + size > ChatStateFrame.MAX_TOTAL) {
      abort(new IllegalStateException("Chat handoff input exceeded bound"));
      return false;
    }
    bytes += size;
    input.add(ReferenceCountUtil.retain(packet));
    return true;
  }

  public boolean sourceChat(VelocityServerConnection sender, ByteBuf packet) {
    if (!active || sender != source || !sourceCut) return false;
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(player.getProtocolVersion().getProtocol());
    if (profile == null || profile.clientbound(ProtocolUtils.readVarInt(packet.duplicate())) != 0x3f) return false;
    if (sourceChat.size() >= 256 || bytes + packet.readableBytes() > ChatStateFrame.MAX_TOTAL) {
      abort(new IllegalStateException("Chat handoff output exceeded bound"));
      return false;
    }
    bytes += packet.readableBytes(); sourceChat.add(packet.retainedDuplicate()); return true;
  }

  /** Source is detached only after RESTORED; old output is discarded, client input resumed normally. */
  public void commit(ClientPlaySessionHandler handler) {
    if (!active || restored == null || !restored.isDone() || restored.isCompletedExceptionally())
      throw new IllegalStateException("Unrestored chat handoff");
    audit("chat.commit", null);
    active = false;
    sourceChat.forEach(ReferenceCountUtil::release); sourceChat.clear();
    try { drainInput(handler); } finally { release(); clear(); }
  }

  public void abort(Throwable failure) {
    if (!active) return;
    audit("chat.abort", failure);
    active = false;
    if (source != null && source.isActive()) {
      send(source, ChatStateFrame.ABORT, new byte[0]);
      ClientPlaySessionHandler handler = (ClientPlaySessionHandler) player.getConnection().getActiveSessionHandler();
      while (!sourceChat.isEmpty()) {
        ByteBuf raw = sourceChat.remove();
        ByteBuf out = handler.canopyChatPacket(raw);
        if (out != raw) raw.release();
        player.getConnection().write(out);
      }
      try { drainInput(handler); } catch (RuntimeException replayFailure) {
        release(); failure.addSuppressed(replayFailure);
        player.disconnect(net.kyori.adventure.text.Component.text("Unable to resume server transfer"));
      }
    } else { release(); }
    if (player.getConnection().getActiveSessionHandler() instanceof ClientPlaySessionHandler play)
      play.getCanopyHandover().cancel();
    if (destination != null) destination.disconnect();
    if (captured != null) captured.completeExceptionally(failure);
    if (restored != null) restored.completeExceptionally(failure);
    clear();
  }

  public void disconnect() {
    if (active) audit("chat.disconnect", null);
    active = false;
    release();
    IllegalStateException failure = new IllegalStateException("Player disconnected during chat handoff");
    if (captured != null) captured.completeExceptionally(failure);
    if (restored != null) restored.completeExceptionally(failure);
    if (destination != null) destination.disconnect();
    clear();
  }

  /** Counts actual encoded payload and reserves the maximum packet-ID width. */
  public static int packetSize(Object packet, ProtocolUtils.Direction direction,
      com.velocitypowered.api.network.ProtocolVersion protocol, int limit) {
    if (packet instanceof ByteBuf raw) return raw.readableBytes();
    if (!(packet instanceof MinecraftPacket decoded)) throw new IllegalArgumentException("Unknown packet type");
    ByteBuf content = packet instanceof io.netty.buffer.ByteBufHolder holder ? holder.content() : null;
    int reader = content == null ? 0 : content.readerIndex();
    ByteBuf sizing = Unpooled.buffer(Math.min(256, limit), limit + 1);
    try { decoded.encode(sizing, direction, protocol); return Math.addExact(5, sizing.readableBytes()); }
    finally { if (content != null) content.readerIndex(reader); sizing.release(); }
  }

  private void drainInput(ClientPlaySessionHandler handler) {
    while (!input.isEmpty()) {
      Object packet = input.remove();
      try {
        if (packet instanceof ByteBuf raw) handler.handleUnknown(raw);
        else if (packet instanceof MinecraftPacket decoded && !decoded.handle(handler)) handler.handleGeneric(decoded);
      } finally { ReferenceCountUtil.release(packet); }
    }
  }
  private void send(VelocityServerConnection target, int operation, byte[] payload) {
    try {
      for (ChatStateFrame frame : binding.fragments(operation, payload)) target.ensureConnected().write(
          new PluginMessagePacket(ChatStateFrame.CHANNEL, Unpooled.wrappedBuffer(frame.encode(key))));
    } catch (Exception failure) { if (active) abort(failure); }
  }
  private void release() {
    input.forEach(ReferenceCountUtil::release); input.clear();
    sourceChat.forEach(ReferenceCountUtil::release); sourceChat.clear();
  }
  private void audit(String event, Throwable failure) {
    CanopyLifecycleHooks.audit(player.getUniqueId(), event,
        "attempt", binding == null ? "" : binding.attempt().toString(),
        "source", source == null ? "" : source.getServerInfo().getName(),
        "destination", destination == null ? "" : destination.getServerInfo().getName(),
        "input-count", input.size(), "source-output-count", sourceChat.size(), "queued-bytes", bytes,
        "error", failure == null ? "" : failure.getClass().getName());
  }

  private void clear() { active = false; sourceCut = false; bytes = 0; snapshot = null; assembly = null;
    source = null; destination = null; binding = null; captured = null; restored = null;
    if (key != null) java.util.Arrays.fill(key, (byte) 0); key = null; }
}
