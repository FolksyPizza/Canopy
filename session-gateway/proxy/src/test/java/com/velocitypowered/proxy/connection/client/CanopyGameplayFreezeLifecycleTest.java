/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class CanopyGameplayFreezeLifecycleTest {

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void connectionCloseDuringConfigurationForgetsFreezeStateForReconnect() throws Exception {
    UUID id = UUID.randomUUID();
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    when(player.getUniqueId()).thenReturn(id);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.MINECRAFT_1_21_11);
    when(player.getUsername()).thenReturn("FixtureClient");

    VelocityServer server = mock(VelocityServer.class);
    VelocityConfiguration configuration = mock(VelocityConfiguration.class);
    when(server.getConfiguration()).thenReturn(configuration);
    when(configuration.isLogPlayerConnections()).thenReturn(false);

    EmbeddedChannel channel = new EmbeddedChannel();
    MinecraftConnection connection = new MinecraftConnection(channel, server);
    channel.pipeline().addLast(connection);
    try {
      connection.setAssociation(player);
      when(player.getConnection()).thenReturn(connection);
      when(player.getProtocolVersion()).thenReturn(ProtocolVersion.MINECRAFT_1_21_11);
      when(player.getUsername()).thenReturn("FixtureClient");
      setField(connection, "state", StateRegistry.PLAY);
      setField(connection, "activeSessionHandler", mock(ClientPlaySessionHandler.class));
      ByteBuf correction = Unpooled.buffer();
      try {
        CanopyGameplayFreeze.writeCorrection(correction, CanopyProtocolProfile.find(774), 47,
            3.5, 70.0, -2.25, 25.0f, 3.0f);
        CanopyGameplayFreeze.observeClientbound(player, correction);
      } finally {
        correction.release();
      }
      assertTrue(CanopyGameplayFreeze.status(List.of(player)).contains("=released(anchor-ready)"));
      assertTrue(freezeAndPump(channel, List.of(player), true).startsWith("Frozen 1"));
      assertTrue(CanopyGameplayFreeze.hasSession(id));
      assertTrue(CanopyGameplayFreeze.status(List.of(player)).contains("=frozen(anchor-ready)"));

      Object session = sessionFor(id);
      assertNotNull(readField(session, "anchor"));
      assertFalse(((Map<?, ?>) readField(session, "genuineTeleports")).isEmpty());
      assertFalse(((Map<?, ?>) readField(session, "syntheticTeleports")).isEmpty());

      // A disconnect can happen while ClientConfigSessionHandler is active; the shared channel
      // close path must clear this UUID even though ClientPlaySessionHandler never runs.
      setField(connection, "state", StateRegistry.CONFIG);
      setField(connection, "activeSessionHandler", mock(ClientConfigSessionHandler.class));
      channel.close().syncUninterruptibly();
      assertFalse(CanopyGameplayFreeze.hasSession(id));
      assertTrue(readField(session, "anchor") == null);
      assertTrue(((Map<?, ?>) readField(session, "genuineTeleports")).isEmpty());
      assertTrue(((Map<?, ?>) readField(session, "syntheticTeleports")).isEmpty());
      assertFalse((boolean) readField(session, "frozen"));

      CanopyGameplayFreeze.beforeDispatch(player, new Object());
      assertTrue(CanopyGameplayFreeze.status(List.of(player)).contains("=released(no-anchor)"));
    } finally {
      channel.finishAndReleaseAll();
      CanopyGameplayFreeze.forget(player);
    }
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void protocol776OverlappingReconnectKeepsReplacementSessionWhenOldChannelCloses() throws Exception {
    UUID id = UUID.randomUUID();
    AtomicReference<ConnectedPlayer> current = new AtomicReference<>();
    VelocityServer server = mock(VelocityServer.class);
    VelocityConfiguration configuration = mock(VelocityConfiguration.class);
    when(server.getConfiguration()).thenReturn(configuration);
    when(configuration.isLogPlayerConnections()).thenReturn(false);
    when(server.getPlayer(id)).thenAnswer(ignored -> Optional.<Player>ofNullable(current.get()));

    ConnectedFixture old = connectedFixture(server, id, "FixtureClient", 776);
    current.set(old.player());
    ByteBuf oldCorrection = Unpooled.buffer();
    try {
      CanopyGameplayFreeze.writeCorrection(oldCorrection, CanopyProtocolProfile.find(776), 71,
          112.25, 81.5, -17.75, 135.0f, -20.0f);
      CanopyGameplayFreeze.observeClientbound(old.player(), oldCorrection);
    } finally {
      oldCorrection.release();
    }
    assertTrue(freezeAndPump(old.channel(), List.of(old.player()), true).startsWith("Frozen 1"));
    Object oldSession = sessionFor(id);
    assertNotNull(readField(oldSession, "anchor"));

    // Velocity's kick-existing login flow registers the replacement before the old channel's
    // inactive callback necessarily runs. The new PLAY packet installs new owner-scoped state.
    ConnectedFixture replacement = connectedFixture(server, id, "FixtureClient", 776);
    current.set(replacement.player());
    assertFalse(CanopyGameplayFreeze.beforeDispatch(replacement.player(), new Object()));
    Object replacementSession = sessionFor(id);
    assertNotNull(replacementSession);
    assertTrue(readField(oldSession, "anchor") == null, "displaced session state was cleared");
    assertTrue(CanopyGameplayFreeze.status(List.of(replacement.player()))
        .contains("=released(no-anchor)"));

    ByteBuf replacementCorrection = Unpooled.buffer();
    try {
      CanopyGameplayFreeze.writeCorrection(replacementCorrection, CanopyProtocolProfile.find(776), 72,
          12.25, 80.5, -7.75, 135.0f, -20.0f);
      CanopyGameplayFreeze.observeClientbound(replacement.player(), replacementCorrection);
    } finally {
      replacementCorrection.release();
    }
    assertTrue(freezeAndPump(replacement.channel(), List.of(replacement.player()), true)
        .startsWith("Frozen 1"));
    assertTrue(CanopyGameplayFreeze.beforeDispatch(replacement.player(), new Object()));

    old.channel().close().syncUninterruptibly();
    assertTrue(CanopyGameplayFreeze.hasSession(id), "old close must not erase replacement UUID entry");
    assertSame(replacementSession, sessionFor(id));
    assertTrue(CanopyGameplayFreeze.status(List.of(replacement.player()))
        .contains("=frozen(anchor-ready)"));
    assertTrue(CanopyGameplayFreeze.beforeDispatch(replacement.player(), new Object()),
        "replacement remains frozen after old owner cleanup");

    current.set(null);
    replacement.channel().close().syncUninterruptibly();
    assertFalse(CanopyGameplayFreeze.hasSession(id), "current connection close still cleans up its state");
    old.channel().finishAndReleaseAll();
    replacement.channel().finishAndReleaseAll();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void frozenProtocol776RelativeCorrectionIsRewrittenInPlaceAndAcknowledgedOnce() throws Exception {
    UUID id = UUID.randomUUID();
    AtomicReference<ConnectedPlayer> current = new AtomicReference<>();
    VelocityServer server = mock(VelocityServer.class);
    VelocityConfiguration configuration = mock(VelocityConfiguration.class);
    when(server.getConfiguration()).thenReturn(configuration);
    when(configuration.isLogPlayerConnections()).thenReturn(false);
    when(server.getPlayer(id)).thenAnswer(ignored -> Optional.<Player>ofNullable(current.get()));

    ConnectedFixture fixture = connectedFixture(server, id, "FixtureClient", 776);
    current.set(fixture.player());
    ByteBuf initial = Unpooled.buffer();
    ByteBuf relative = null;
    try {
      assertFalse(CanopyGameplayFreeze.beforeDispatch(fixture.player(), new Object()));
      CanopyGameplayFreeze.writeCorrection(initial, CanopyProtocolProfile.find(776), 81,
          100.0, 70.0, -50.0, 90.0f, 10.0f);
      CanopyGameplayFreeze.observeClientbound(fixture.player(), initial);
      assertTrue(freezeAndPump(fixture.channel(), List.of(fixture.player()), true).startsWith("Frozen 1"));
      releaseOutbound(fixture.channel());

      int teleportId = 93;
      Object session = sessionFor(id);
      Map<?, ?> syntheticBefore = new java.util.HashMap<>((Map<?, ?>) readField(session, "syntheticTeleports"));
      relative = relativePositionCorrection(teleportId, 1.25, -0.5, 2.0,
          0.125, -0.25, 0.5, 15.0f, -5.0f, 0x1ff);
      CanopyGameplayFreeze.observeClientbound(fixture.player(), relative);
      assertEquals(syntheticBefore, readField(session, "syntheticTeleports"),
          "forwarding the backend teleport must not allocate a second synthetic ACK identity");
      assertTrue(((Map<?, ?>) readField(session, "genuineTeleports")).containsKey(teleportId),
          "the original backend teleport id remains tracked as genuine");
      assertFalse(syntheticBefore.containsKey(teleportId));
      assertEquals(0, relative.readerIndex(), "observation leaves the forwarded packet readable");
      assertNull(fixture.channel().readOutbound(),
          "observing a frozen backend correction must not send an overtaking synthetic packet");

      ByteBuf decoded = relative.duplicate();
      assertEquals(CanopyProtocolProfile.find(776).clientboundWire(0x46), ProtocolUtils.readVarInt(decoded));
      assertEquals(teleportId, ProtocolUtils.readVarInt(decoded), "original genuine teleport id is retained");
      assertEquals(101.25, decoded.readDouble());
      assertEquals(69.5, decoded.readDouble());
      assertEquals(-48.0, decoded.readDouble());
      assertEquals(0.125, decoded.readDouble());
      assertEquals(-0.25, decoded.readDouble());
      assertEquals(0.5, decoded.readDouble());
      assertEquals(105.0f, decoded.readFloat());
      assertEquals(5.0f, decoded.readFloat());
      assertEquals(0x1e0L, decoded.readUnsignedInt(), "only position/rotation relative flags are cleared");
      assertFalse(decoded.isReadable());

      ByteBuf genuineAck = Unpooled.buffer();
      try {
        ProtocolUtils.writeVarInt(genuineAck, CanopyProtocolProfile.find(776).serverboundWire(0x00));
        ProtocolUtils.writeVarInt(genuineAck, teleportId);
        assertFalse(CanopyGameplayFreeze.beforeDispatch(fixture.player(), genuineAck),
            "the backend's original teleport acknowledgement must pass the frozen gate");
      } finally {
        genuineAck.release();
      }

      fixture.connection().delayedWrite(relative.retain());
      fixture.connection().flush();
      ByteBuf forwarded = fixture.channel().readOutbound();
      assertNotNull(forwarded, "the backend handler can queue the same rewritten packet after observation");
      try {
        assertEquals(ByteBufUtil.hexDump(relative), ByteBufUtil.hexDump(forwarded));
      } finally {
        forwarded.release();
      }
      assertNull(fixture.channel().readOutbound(), "exactly one correction is queued");
    } finally {
      if (relative != null) relative.release();
      initial.release();
      CanopyGameplayFreeze.forget(fixture.player());
      current.set(null);
      fixture.channel().finishAndReleaseAll();
    }
  }

  @Test
  void partialGroupReleaseRefreezesSessionsThatAlreadyReleased() {
    Fixture first = fixture("FixtureOne");
    Fixture second = fixture("FixtureTwo");
    try {
      assertFalse(CanopyGameplayFreeze.beforeDispatch(first.player(), new Object()));
      assertFalse(CanopyGameplayFreeze.beforeDispatch(second.player(), new Object()));
      observeAnchor(first.player());
      observeAnchor(second.player());

      String frozen = CanopyGameplayFreeze.setFrozen(List.of(first.player(), second.player()), true).join();
      assertTrue(frozen.startsWith("Frozen 2"), frozen);

      // Both pass preflight; second transitions out of PLAY only when its release runs. The first
      // release therefore succeeds before the second one fails, forcing the group rollback path.
      when(first.connection().getState()).thenReturn(StateRegistry.PLAY, StateRegistry.PLAY, StateRegistry.PLAY);
      when(second.connection().getState()).thenReturn(
          StateRegistry.PLAY, StateRegistry.CONFIG, StateRegistry.PLAY);
      String released = CanopyGameplayFreeze.setFrozen(List.of(first.player(), second.player()), false).join();
      assertTrue(released.startsWith("Release incomplete; previously frozen sessions were re-frozen"), released);
      assertTrue(CanopyGameplayFreeze.status(List.of(first.player(), second.player()))
          .contains("FixtureOne=frozen(anchor-ready)"));
      assertTrue(CanopyGameplayFreeze.status(List.of(first.player(), second.player()))
          .contains("FixtureTwo=frozen(anchor-ready)"));
    } finally {
      CanopyGameplayFreeze.forget(first.player());
      CanopyGameplayFreeze.forget(second.player());
    }
  }

  private static ByteBuf relativePositionCorrection(int teleportId, double x, double y, double z,
      double deltaX, double deltaY, double deltaZ, float yaw, float pitch, int flags) {
    CanopyProtocolProfile profile = CanopyProtocolProfile.find(776);
    ByteBuf packet = Unpooled.buffer();
    ProtocolUtils.writeVarInt(packet, profile.clientboundWire(0x46));
    ProtocolUtils.writeVarInt(packet, teleportId);
    packet.writeDouble(x).writeDouble(y).writeDouble(z);
    packet.writeDouble(deltaX).writeDouble(deltaY).writeDouble(deltaZ);
    packet.writeFloat(yaw).writeFloat(pitch).writeInt(flags);
    return packet;
  }

  private static void releaseOutbound(EmbeddedChannel channel) {
    Object message;
    while ((message = channel.readOutbound()) != null) {
      ReferenceCountUtil.release(message);
    }
  }

  private static void observeAnchor(ConnectedPlayer player) {
    ByteBuf correction = Unpooled.buffer();
    try {
      CanopyGameplayFreeze.writeCorrection(correction, CanopyProtocolProfile.find(774), 47,
          3.5, 70.0, -2.25, 25.0f, 3.0f);
      CanopyGameplayFreeze.observeClientbound(player, correction);
    } finally {
      correction.release();
    }
  }

  private static ConnectedFixture connectedFixture(VelocityServer server, UUID id, String username,
      int protocol) throws Exception {
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    when(player.getUniqueId()).thenReturn(id);
    when(player.getUsername()).thenReturn(username);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
    EmbeddedChannel channel = new EmbeddedChannel();
    MinecraftConnection connection = new MinecraftConnection(channel, server);
    channel.pipeline().addLast(connection);
    connection.setAssociation(player);
    setField(connection, "state", StateRegistry.PLAY);
    setField(connection, "activeSessionHandler", mock(ClientPlaySessionHandler.class));
    when(player.getConnection()).thenReturn(connection);
    return new ConnectedFixture(player, connection, channel);
  }

  private static String freezeAndPump(EmbeddedChannel channel, List<? extends Player> players,
      boolean frozen) throws Exception {
    CompletableFuture<String> result = CanopyGameplayFreeze.setFrozen(players, frozen);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!result.isDone() && System.nanoTime() < deadline) {
      channel.runPendingTasks();
      channel.runScheduledPendingTasks();
      Thread.yield();
    }
    assertTrue(result.isDone(), "embedded event-loop task completed before the bounded wait");
    return result.get(1, TimeUnit.SECONDS);
  }

  private static void setField(MinecraftConnection connection, String name, Object value) throws Exception {
    Field field = MinecraftConnection.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(connection, value);
  }

  private static Object sessionFor(UUID id) throws Exception {
    Field field = CanopyGameplayFreeze.class.getDeclaredField("SESSIONS");
    field.setAccessible(true);
    return ((Map<?, ?>) field.get(null)).get(id);
  }

  private static Object readField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static Fixture fixture(String username) {
    UUID id = UUID.randomUUID();
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    when(player.getUniqueId()).thenReturn(id);
    when(player.getUsername()).thenReturn(username);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.MINECRAFT_1_21_11);

    Channel channel = mock(Channel.class);
    when(channel.isActive()).thenReturn(true);
    when(channel.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    EventLoop eventLoop = mock(EventLoop.class);
    doAnswer(invocation -> {
      ((Runnable) invocation.getArgument(0)).run();
      return null;
    }).when(eventLoop).execute(any(Runnable.class));

    MinecraftConnection connection = mock(MinecraftConnection.class);
    when(connection.getChannel()).thenReturn(channel);
    when(connection.eventLoop()).thenReturn(eventLoop);
    when(connection.getState()).thenReturn(StateRegistry.PLAY);
    when(connection.getActiveSessionHandler()).thenReturn(mock(ClientPlaySessionHandler.class));
    doAnswer(invocation -> {
      ReferenceCountUtil.release(invocation.getArgument(0));
      return mock(ChannelFuture.class);
    }).when(connection).write(any());
    when(player.getConnection()).thenReturn(connection);
    return new Fixture(player, connection);
  }

  private record Fixture(ConnectedPlayer player, MinecraftConnection connection) { }
  private record ConnectedFixture(ConnectedPlayer player, MinecraftConnection connection, EmbeddedChannel channel) { }
}
