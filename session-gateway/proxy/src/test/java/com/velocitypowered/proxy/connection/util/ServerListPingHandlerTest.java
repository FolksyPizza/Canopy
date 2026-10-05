/*
 * Copyright (C) 2026 Canopy Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.velocitypowered.proxy.connection.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.ServerPing;
import org.junit.jupiter.api.Test;

class ServerListPingHandlerTest {
  @Test
  void localCanopyBrandKeepsTheClientProtocolNumber() {
    ServerPing.Version version = ServerListPingHandler.localVersion(
        ProtocolVersion.getProtocolVersion(774));

    assertEquals(774, version.getProtocol());
    assertTrue(version.getName().startsWith("Canopy "));
  }

  @Test
  void unknownClientUsesMaximumSupportedProtocol() {
    ServerPing.Version version = ServerListPingHandler.localVersion(ProtocolVersion.UNKNOWN);

    assertEquals(ProtocolVersion.MAXIMUM_VERSION.getProtocol(), version.getProtocol());
    assertTrue(version.getName().startsWith("Canopy "));
  }
}
