/*
 * Copyright (C) 2026 Canopy Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.velocitypowered.proxy.connection.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.connection.client.CanopyProtocolProfile;
import com.velocitypowered.proxy.connection.client.CanopySwitchPolicy;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LoginSessionHandlerCanopySelectionTest {
  @Test
  void failoverUsesAttemptPreviousServerAfterCurrentConnectionWasCleared() {
    UUID playerId = UUID.randomUUID();
    try (PropertyRestore ignored = seamlessWithMatchingFingerprints(playerId)) {
      ConnectedPlayer player = mock(ConnectedPlayer.class);
      when(player.getUniqueId()).thenReturn(playerId);
      when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(774));
      when(player.getCanopyConfiguration()).thenReturn("b".repeat(64));
      when(player.getConnectedServer()).thenReturn(null);

      RegisteredServer previous = mock(RegisteredServer.class);
      when(previous.getServerInfo()).thenReturn(serverInfo("source"));
      VelocityServerConnection destination = mock(VelocityServerConnection.class);
      when(destination.getPreviousServer()).thenReturn(Optional.of(previous));
      when(destination.getServerInfo()).thenReturn(serverInfo("destination"));

      assertEquals(CanopySwitchPolicy.Mode.SEAMLESS,
          LoginSessionHandler.selectCanopyMode(player, destination));
    }
  }

  @Test
  void staleSourceConfigurationFallsBackBeforeSeamlessSwitch() {
    UUID playerId = UUID.randomUUID();
    try (PropertyRestore ignored = seamlessWithMatchingFingerprints(playerId)) {
      ConnectedPlayer player = mock(ConnectedPlayer.class);
      when(player.getUniqueId()).thenReturn(playerId);
      when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(774));
      when(player.getCanopyConfiguration()).thenReturn("c".repeat(64));

      RegisteredServer previous = mock(RegisteredServer.class);
      when(previous.getServerInfo()).thenReturn(serverInfo("source"));
      VelocityServerConnection destination = mock(VelocityServerConnection.class);
      when(destination.getPreviousServer()).thenReturn(Optional.of(previous));
      when(destination.getServerInfo()).thenReturn(serverInfo("destination"));

      assertEquals(CanopySwitchPolicy.Mode.NORMAL,
          LoginSessionHandler.selectCanopyMode(player, destination));
    }
  }

  @Test
  void firstLoginRemainsNormalWithoutPreviousServerMetadata() {
    UUID playerId = UUID.randomUUID();
    try (PropertyRestore ignored = seamlessWithMatchingFingerprints(playerId)) {
      ConnectedPlayer player = mock(ConnectedPlayer.class);
      when(player.getUniqueId()).thenReturn(playerId);
      when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(774));

      VelocityServerConnection destination = mock(VelocityServerConnection.class);
      when(destination.getPreviousServer()).thenReturn(Optional.empty());
      when(destination.getServerInfo()).thenReturn(serverInfo("destination"));

      assertEquals(CanopySwitchPolicy.Mode.NORMAL,
          LoginSessionHandler.selectCanopyMode(player, destination));
    }
  }

  @Test
  void parityApprovalIsScopedToEachSupportedNativeProtocol() {
    UUID playerId = UUID.randomUUID();
    List<String> properties = new ArrayList<>(List.of(
        "canopy.mode", "SEAMLESS",
        "canopy.modeFallback", "NORMAL",
        "canopy.configurationFingerprint.source", "a".repeat(64),
        "canopy.configurationFingerprint.destination", "a".repeat(64)));
    for (int protocol : CanopyProtocolProfile.supportedProtocols()) {
      String fingerprint = protocol == 776 ? "b".repeat(64) : "";
      properties.add("canopy.configurationFingerprint." + protocol + ".source");
      properties.add(fingerprint);
      properties.add("canopy.configurationFingerprint." + protocol + ".destination");
      properties.add(fingerprint);
    }
    try (PropertyRestore ignored = new PropertyRestore(playerId, properties.toArray(String[]::new))) {
      ConnectedPlayer player = mock(ConnectedPlayer.class);
      when(player.getUniqueId()).thenReturn(playerId);
      when(player.getConnectedServer()).thenReturn(null);
      when(player.getCanopyConfiguration()).thenReturn("b".repeat(64));

      RegisteredServer previous = mock(RegisteredServer.class);
      when(previous.getServerInfo()).thenReturn(serverInfo("source"));
      VelocityServerConnection destination = mock(VelocityServerConnection.class);
      when(destination.getPreviousServer()).thenReturn(Optional.of(previous));
      when(destination.getServerInfo()).thenReturn(serverInfo("destination"));
      for (int protocol : CanopyProtocolProfile.supportedProtocols()) {
        when(player.getCanopyConfiguration()).thenReturn("b".repeat(64));
        when(player.getProtocolVersion()).thenReturn(ProtocolVersion.getProtocolVersion(protocol));
        CanopySwitchPolicy.markPlatform(playerId, CanopySwitchPolicy.Platform.JAVA);
        CanopySwitchPolicy.Mode expected = protocol == 776
            ? CanopySwitchPolicy.Mode.SEAMLESS : CanopySwitchPolicy.Mode.NORMAL;
        assertEquals(expected, LoginSessionHandler.selectCanopyMode(player, destination),
            "only protocol 776 is approved; legacy unscoped values cannot authorize protocol " + protocol);

        String sourceKey = "canopy.configurationFingerprint." + protocol + ".source";
        String destinationKey = "canopy.configurationFingerprint." + protocol + ".destination";
        System.setProperty(sourceKey, "c".repeat(64));
        System.setProperty(destinationKey, "c".repeat(64));
        when(player.getCanopyConfiguration()).thenReturn("c".repeat(64));
        assertEquals(CanopySwitchPolicy.Mode.SEAMLESS,
            LoginSessionHandler.selectCanopyMode(player, destination),
            "a matching approval for protocol " + protocol + " enables only that protocol's session");
      }
    }
  }

  private static PropertyRestore seamlessWithMatchingFingerprints(UUID playerId) {
    PropertyRestore restore = new PropertyRestore(playerId,
        "canopy.mode", "SEAMLESS",
        "canopy.modeFallback", "NORMAL",
        "canopy.configurationFingerprint.774.source", "b".repeat(64),
        "canopy.configurationFingerprint.774.destination", "b".repeat(64));
    CanopySwitchPolicy.markPlatform(playerId, CanopySwitchPolicy.Platform.JAVA);
    return restore;
  }

  private static ServerInfo serverInfo(String name) {
    return new ServerInfo(name, new InetSocketAddress("127.0.0.1", 25565));
  }

  private record PropertyRestore(UUID playerId, List<String> names, List<String> values)
      implements AutoCloseable {
    PropertyRestore(UUID playerId, String... nameValues) {
      this(playerId, names(nameValues), values(nameValues));
      for (int index = 0; index < nameValues.length; index += 2) {
        System.setProperty(nameValues[index], nameValues[index + 1]);
      }
    }

    private static List<String> names(String[] nameValues) {
      List<String> names = new ArrayList<>();
      for (int index = 0; index < nameValues.length; index += 2) names.add(nameValues[index]);
      return names;
    }

    private static List<String> values(String[] nameValues) {
      List<String> values = new ArrayList<>();
      for (int index = 0; index < nameValues.length; index += 2) {
        values.add(System.getProperty(nameValues[index]));
      }
      return values;
    }

    @Override
    public void close() {
      CanopySwitchPolicy.forget(playerId);
      for (int index = 0; index < names.size(); index++) {
        if (values.get(index) == null) System.clearProperty(names.get(index));
        else System.setProperty(names.get(index), values.get(index));
      }
    }
  }
}
