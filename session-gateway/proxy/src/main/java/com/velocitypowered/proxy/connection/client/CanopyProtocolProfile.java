package com.velocitypowered.proxy.connection.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Explicit wire packet tables. Unknown future protocols never inherit another version's IDs. */
public final class CanopyProtocolProfile {
  private static final Map<Integer, CanopyProtocolProfile> PROFILES = load();
  private final Map<Integer, Integer> clientbound = new HashMap<>();
  private final Map<Integer, Integer> serverbound = new HashMap<>();
  private final Map<Integer, Integer> clientboundWire = new HashMap<>();
  private final Map<Integer, Integer> serverboundWire = new HashMap<>();
  private final int protocol;

  private CanopyProtocolProfile(int protocol) {
    this.protocol = protocol;
  }

  public static CanopyProtocolProfile find(int protocol) {
    return PROFILES.get(protocol);
  }

  public int protocol() { return protocol; }
  public int clientbound(int wireId) { return clientbound.getOrDefault(wireId, -1); }
  public int serverbound(int wireId) { return serverbound.getOrDefault(wireId, -1); }
  public int clientboundWire(int canonical) { return required(clientboundWire, canonical); }
  public int serverboundWire(int canonical) { return required(serverboundWire, canonical); }

  private static int required(Map<Integer, Integer> ids, int canonical) {
    Integer wire = ids.get(canonical);
    if (wire == null) {
      throw new IllegalArgumentException("No packet mapping for " + canonical);
    }
    return wire;
  }

  private static Map<Integer, CanopyProtocolProfile> load() {
    Map<Integer, CanopyProtocolProfile> profiles = new HashMap<>();
    var resource = CanopyProtocolProfile.class.getResourceAsStream("/canopy/protocol-profiles.tsv");
    if (resource == null) {
      throw new IllegalStateException("Missing Canopy protocol tables");
    }
    try (var reader = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank() || line.startsWith("#")) { continue; }
        String[] row = line.split(" ");
        int protocol = Integer.parseInt(row[0]);
        int canonical = Integer.parseInt(row[2], 16);
        int wire = Integer.parseInt(row[3], 16);
        var profile = profiles.computeIfAbsent(protocol, CanopyProtocolProfile::new);
        boolean cb = row[1].equals("CB");
        if ((cb ? profile.clientbound : profile.serverbound).put(wire, canonical) != null
            || (cb ? profile.clientboundWire : profile.serverboundWire).put(canonical, wire) != null) {
          throw new IllegalStateException("Duplicate Canopy packet mapping");
        }
      }
      return Map.copyOf(profiles);
    } catch (IOException | NumberFormatException failure) {
      throw new IllegalStateException("Invalid Canopy protocol tables", failure);
    }
  }
}
