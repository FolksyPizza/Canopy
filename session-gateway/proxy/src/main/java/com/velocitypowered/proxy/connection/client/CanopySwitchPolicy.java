package com.velocitypowered.proxy.connection.client;

import java.util.Locale;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Selects the client transition once, before the destination starts configuration. */
public final class CanopySwitchPolicy {
  public enum Mode { NORMAL, FAST, SEAMLESS }
  public enum Fallback { REJECT, NORMAL, FAST }

  private static final ConcurrentHashMap<UUID, Mode> sessionModes = new ConcurrentHashMap<>();
  private static final Set<UUID> normalRefresh = ConcurrentHashMap.newKeySet();

  private CanopySwitchPolicy() { }

  public static Mode requested(UUID player) {
    return normalRefresh.contains(player) ? Mode.NORMAL : sessionMode(player);
  }

  public static Mode sessionMode(UUID player) { return sessionModes.getOrDefault(player, requested()); }
  public static Mode override(UUID player) { return sessionModes.get(player); }
  public static boolean isRefreshing(UUID player) { return normalRefresh.contains(player); }
  public static void restoreOverride(UUID player, Mode mode) {
    if (mode == null) sessionModes.remove(player); else sessionModes.put(player, mode);
  }

  /** Changes the next attachment; the current client's entity map remains valid until reset. */
  public static void seamless(UUID player, boolean enabled) {
    sessionModes.put(player, enabled ? Mode.SEAMLESS : Mode.NORMAL);
  }

  public static void beginRefresh(UUID player) {
    if (!normalRefresh.add(player)) throw new IllegalStateException("A refresh is already running");
  }

  public static void finishRefresh(UUID player) { normalRefresh.remove(player); }

  public static void forget(UUID player) {
    normalRefresh.remove(player);
    sessionModes.remove(player);
  }

  public static Mode requested() {
    String configured = System.getProperty("canopy.mode");
    if (configured != null) {
      return Mode.valueOf(configured.trim().toUpperCase(Locale.ROOT));
    }
    if (Boolean.getBoolean("canopy.noRespawn")) {
      return Mode.SEAMLESS;
    }
    return Boolean.getBoolean("canopy.seamless") ? Mode.FAST : Mode.NORMAL;
  }

  /** Updates the process-wide default used for the next backend attachment. */
  public static void setGlobalSeamless(boolean enabled) {
    System.setProperty("canopy.mode", enabled ? Mode.SEAMLESS.name() : Mode.NORMAL.name());
  }

  /** Returns whether new backend attachments default to seamless mode. */
  public static boolean globalSeamless() {
    return requested() == Mode.SEAMLESS;
  }

  public static Fallback fallback() {
    return Fallback.valueOf(System.getProperty("canopy.modeFallback", "REJECT")
        .trim().toUpperCase(Locale.ROOT));
  }

  public static String configurationFingerprint(String backend) {
    return System.getProperty("canopy.configurationFingerprint." + backend, "");
  }

  /** Fingerprints cover registries, tags, packs and all client-visible configuration. */
  public static boolean sameConfiguration(String source, String destination) {
    return source != null && source.matches("(?i)[0-9a-f]{64}")
        && source.equalsIgnoreCase(destination);
  }

  public static Mode select(Mode requested, Fallback fallback, boolean initialLogin,
      boolean redundantConfiguration, boolean translationSupported) {
    if (initialLogin || requested == Mode.NORMAL) {
      return Mode.NORMAL;
    }
    if (requested == Mode.FAST) {
      return redundantConfiguration ? Mode.FAST : Mode.NORMAL;
    }
    if (redundantConfiguration && translationSupported) {
      return Mode.SEAMLESS;
    }
    return switch (fallback) {
      case NORMAL -> Mode.NORMAL;
      case FAST -> redundantConfiguration ? Mode.FAST : Mode.NORMAL;
      case REJECT -> throw new IllegalStateException(
          "SEAMLESS requires matching configuration fingerprints and a supported translation profile");
    };
  }
}
