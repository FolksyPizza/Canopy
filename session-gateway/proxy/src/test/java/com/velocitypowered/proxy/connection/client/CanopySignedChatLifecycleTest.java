/* Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later */
package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class CanopySignedChatLifecycleTest {
  private static void field(Object object, String name, Object value) throws Exception {
    var field = object.getClass().getDeclaredField(name);
    field.setAccessible(true); field.set(object, value);
  }

  private static class Fixture {
    final ConnectedPlayer player = mock(ConnectedPlayer.class);
    final VelocityServerConnection source = mock(VelocityServerConnection.class);
    final VelocityServerConnection destination = mock(VelocityServerConnection.class);
    final MinecraftConnection client = mock(MinecraftConnection.class);
    final MinecraftConnection sourceWire = mock(MinecraftConnection.class);
    final ClientPlaySessionHandler play = mock(ClientPlaySessionHandler.class);
    final CanopySignedChatHandoff bridge = new CanopySignedChatHandoff(player);
    final UUID attempt = UUID.randomUUID();

    Fixture() throws Exception {
      when(player.getConnection()).thenReturn(client);
      when(player.getConnectedServer()).thenReturn(source);
      when(player.getUniqueId()).thenReturn(UUID.randomUUID());
      when(source.isActive()).thenReturn(true); when(destination.isActive()).thenReturn(true);
      when(source.getServerInfo()).thenReturn(new ServerInfo("source", new InetSocketAddress("127.0.0.1", 1)));
      when(destination.getServerInfo()).thenReturn(new ServerInfo("destination", new InetSocketAddress("127.0.0.1", 2)));
      when(source.ensureConnected()).thenReturn(sourceWire);
      when(sourceWire.write(any())).thenAnswer(call -> { ReferenceCountUtil.release(call.getArgument(0)); return null; });
      when(client.getActiveSessionHandler()).thenReturn(play);
      when(play.getCanopyHandover()).thenReturn(mock(CanopyHandover.class));
      field(bridge, "active", true); field(bridge, "source", source); field(bridge, "destination", destination);
      field(bridge, "restored", CompletableFuture.completedFuture(null)); field(bridge, "key", new byte[32]);
      field(bridge, "binding", new ChatStateFrame(ChatStateFrame.CAPTURE, attempt, player.getUniqueId(),
          "source", "destination", System.currentTimeMillis() + 30000, 0, 1, 0, new byte[0]));
    }
  }

  @Test void eventWaitAbortLeavesNativeSourceAttached() throws Exception {
    var fixture = new Fixture();
    var connectedEvent = new CompletableFuture<Void>();
    Runnable detachAndJoin = mock(Runnable.class);
    var finished = fixture.bridge.finishAfter(connectedEvent, fixture.destination, Runnable::run, detachAndJoin);
    assertFalse(finished.isDone());
    fixture.bridge.abort(new IllegalStateException("Timed out while connection event pending"));
    assertSame(fixture.source, fixture.player.getConnectedServer());
    verify(fixture.source, never()).disconnect(); verify(fixture.destination).disconnect();
    connectedEvent.complete(null);
    assertTrue(finished.isCompletedExceptionally()); verify(detachAndJoin, never()).run();
  }

  @Test void successfulEventStillCannotDetachIfAbortWinsQueuedCallback() throws Exception {
    var fixture = new Fixture();
    var connectedEvent = new CompletableFuture<Void>();
    var eventLoop = new ArrayDeque<Runnable>();
    Runnable detachAndJoin = mock(Runnable.class);
    var finished = fixture.bridge.finishAfter(connectedEvent, fixture.destination, eventLoop::add, detachAndJoin);
    connectedEvent.complete(null); assertEquals(1, eventLoop.size());
    fixture.bridge.abort(new IllegalStateException("Timeout won event-loop race"));
    eventLoop.remove().run();
    assertTrue(finished.isCompletedExceptionally()); verify(detachAndJoin, never()).run();
    assertSame(fixture.source, fixture.player.getConnectedServer()); verify(fixture.source, never()).disconnect();
  }

  @Test void previousAttemptDrainCannotSendOrAbortCurrentAttempt() throws Exception {
    var fixture = new Fixture(); UUID previous = UUID.randomUUID();
    fixture.bridge.drainCompleted(previous, null);
    fixture.bridge.drainCompleted(previous, new IllegalStateException("Old queue failed"));
    assertTrue(fixture.bridge.currentAttempt(fixture.attempt));
    verify(fixture.sourceWire, never()).write(any()); verify(fixture.destination, never()).disconnect();
    fixture.bridge.disconnect();
  }

  @Test void obsoleteEventFailureCannotAbortNewAttemptOnSameSource() throws Exception {
    var fixture = new Fixture();
    var connectedEvent = new CompletableFuture<Void>();
    Runnable detachAndJoin = mock(Runnable.class);
    var finished = fixture.bridge.finishAfter(fixture.attempt, connectedEvent,
        fixture.destination, Runnable::run, detachAndJoin);
    var cleaned = finished.exceptionally(failure -> {
      fixture.bridge.abortAttempt(fixture.attempt, fixture.destination, failure); return null;
    });
    fixture.bridge.abort(new IllegalStateException("First attempt ended"));
    UUID nextAttempt = UUID.randomUUID();
    var nextTarget = mock(VelocityServerConnection.class);
    when(nextTarget.isActive()).thenReturn(true);
    when(nextTarget.getServerInfo()).thenReturn(new ServerInfo("next", new InetSocketAddress("127.0.0.1", 3)));
    field(fixture.bridge, "active", true); field(fixture.bridge, "source", fixture.source);
    field(fixture.bridge, "destination", nextTarget); field(fixture.bridge, "key", new byte[32]);
    field(fixture.bridge, "binding", new ChatStateFrame(ChatStateFrame.CAPTURE, nextAttempt,
        fixture.player.getUniqueId(), "source", "next", System.currentTimeMillis() + 30000,
        0, 1, 0, new byte[0]));
    connectedEvent.completeExceptionally(new IllegalStateException("Old event listener failed late"));
    assertTrue(cleaned.isDone()); assertTrue(fixture.bridge.currentAttempt(nextAttempt));
    assertFalse(fixture.bridge.ownsAttempt(fixture.attempt, fixture.destination));
    verify(nextTarget, never()).disconnect(); verify(fixture.source, never()).disconnect();
    verify(detachAndJoin, never()).run();
    fixture.bridge.disconnect();
  }

  @Test void obsoletePreFinishFailureAfterNewBackendCommittedCannotKickPlayer() throws Exception {
    var fixture = new Fixture();
    fixture.bridge.abort(new IllegalStateException("Original attempt ended"));
    var nextBackend = mock(VelocityServerConnection.class);
    when(fixture.player.getConnectedServer()).thenReturn(nextBackend);
    assertFalse(fixture.bridge.shouldRemediateFailure(fixture.attempt, fixture.destination,
        fixture.source, false, false));
    assertFalse(fixture.bridge.shouldRemediateFailure(fixture.attempt, fixture.destination,
        fixture.source, true, false));
    verify(fixture.source, never()).disconnect();
    verify(fixture.player, never()).disconnect(any());
  }

  @Test void genuineHalfFinishedDetachFailureStillRequiresFailClosedHandling() throws Exception {
    var fixture = new Fixture();
    when(fixture.player.getConnectedServer()).thenReturn(null);
    assertTrue(fixture.bridge.shouldRemediateFailure(fixture.attempt, fixture.destination,
        fixture.source, true, false));
    // A different in-flight transaction owns the player even if current is temporarily null.
    when(fixture.player.getConnectionInFlight()).thenReturn(mock(VelocityServerConnection.class));
    assertFalse(fixture.bridge.shouldRemediateFailure(fixture.attempt, fixture.destination,
        fixture.source, true, false));
    when(fixture.player.getConnectionInFlight()).thenReturn(fixture.destination);
    // Once native commit succeeded, later event errors cannot undo it or kick this session.
    assertFalse(fixture.bridge.shouldRemediateFailure(fixture.attempt, fixture.destination,
        fixture.source, true, true));
    fixture.bridge.disconnect();
  }

  @Test void configAndInitialClientsAlwaysConsumeReservedChannel() {
    var packet = new PluginMessagePacket(ChatStateFrame.CHANNEL, Unpooled.buffer().writeByte(1));
    try {
      assertTrue(new ClientConfigSessionHandler(null, null).handle(packet));
      assertTrue(new InitialConnectSessionHandler(null, null).handle(packet));
      assertEquals(1, packet.refCnt());
    } finally { packet.release(); }
  }
}
