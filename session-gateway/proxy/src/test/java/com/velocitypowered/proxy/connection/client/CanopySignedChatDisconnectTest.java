/* Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later */
package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** A client that disconnects mid-handoff ends the signed chat attempt at once, not at its deadline. */
class CanopySignedChatDisconnectTest {
  private static void field(Object object, String name, Object value) throws Exception {
    var field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(object, value);
  }

  @Test
  void clientDisconnectAbortsActiveChatHandoff() throws Exception {
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    MinecraftConnection client = mock(MinecraftConnection.class);
    when(player.getUniqueId()).thenReturn(UUID.randomUUID());
    when(player.getUsername()).thenReturn("FixtureClient");
    when(player.getConnection()).thenReturn(client);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(774));
    when(client.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(774));
    ClientPlaySessionHandler handler = new ClientPlaySessionHandler(mock(VelocityServer.class), player);
    when(client.getActiveSessionHandler()).thenReturn(handler);

    CanopySignedChatHandoff handoff = handler.signedChat();
    CompletableFuture<Void> captured = new CompletableFuture<>();
    field(handoff, "active", true);
    field(handoff, "captured", captured);

    handler.disconnected();

    assertTrue(captured.isCompletedExceptionally(), "the pending capture must fail on disconnect");
    assertFalse(handoff.active(), "the handoff must not stay active");
  }
}
