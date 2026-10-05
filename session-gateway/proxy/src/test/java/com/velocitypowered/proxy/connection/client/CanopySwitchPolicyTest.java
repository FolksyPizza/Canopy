package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;
import static com.velocitypowered.proxy.connection.client.CanopySwitchPolicy.*;

import org.junit.jupiter.api.Test;

class CanopySwitchPolicyTest {
  @Test void statusReportsRefreshAndRestoresFailedToggleWithoutSessionLeaks() {
    java.util.UUID id = java.util.UUID.randomUUID();
    assertNull(override(id));
    assertFalse(isRefreshing(id));
    Mode initial = sessionMode(id);
    seamless(id, false);
    assertEquals(Mode.NORMAL, sessionMode(id));
    beginRefresh(id);
    assertTrue(isRefreshing(id));
    restoreOverride(id, null);
    assertEquals(initial, sessionMode(id));
    assertEquals(Mode.NORMAL, requested(id));
    forget(id);
    assertFalse(isRefreshing(id));
    assertNull(override(id));
  }

  @Test void normalRefreshRestoresSessionModeAndDoesNotLeakToOtherPlayers() {
    var a = java.util.UUID.randomUUID(); var b = java.util.UUID.randomUUID();
    try {
      markPlatform(a, Platform.JAVA); markPlatform(b, Platform.JAVA);
      seamless(a, true); seamless(b, false); beginRefresh(a);
      assertEquals(Mode.NORMAL, requested(a));
      assertEquals(Mode.NORMAL, requested(b));
      assertThrows(IllegalStateException.class, () -> beginRefresh(a));
      finishRefresh(a);
      assertEquals(Mode.SEAMLESS, requested(a));
      assertEquals(Mode.NORMAL, requested(b));
      seamless(a, false); beginRefresh(a); finishRefresh(a);
      assertEquals(Mode.NORMAL, requested(a));
    } finally { forget(a); forget(b); }
  }

  @Test void firstLoginAlwaysInitializesClientWorld() {
    for (Mode mode : Mode.values()) {
      assertEquals(Mode.NORMAL, select(mode, Fallback.REJECT, true, false, false));
    }
  }

  @Test void normalNeverReusesConfiguration() {
    assertEquals(Mode.NORMAL, select(Mode.NORMAL, Fallback.REJECT, false, true, true));
  }

  @Test void fastOnlySkipsMatchingConfiguration() {
    assertEquals(Mode.FAST, select(Mode.FAST, Fallback.REJECT, false, true, false));
    assertEquals(Mode.NORMAL, select(Mode.FAST, Fallback.REJECT, false, false, true));
  }

  @Test void seamlessDoesNotSilentlyRespawn() {
    assertEquals(Mode.SEAMLESS, select(Mode.SEAMLESS, Fallback.REJECT, false, true, true));
    assertThrows(IllegalStateException.class,
        () -> select(Mode.SEAMLESS, Fallback.REJECT, false, true, false));
    assertThrows(IllegalStateException.class,
        () -> select(Mode.SEAMLESS, Fallback.REJECT, false, false, true));
  }

  @Test void seamlessFallbackRequiresExplicitSelectionAndHonorsConfiguration() {
    assertEquals(Mode.FAST, select(Mode.SEAMLESS, Fallback.FAST, false, true, false));
    assertEquals(Mode.NORMAL, select(Mode.SEAMLESS, Fallback.FAST, false, false, true));
    assertEquals(Mode.NORMAL, select(Mode.SEAMLESS, Fallback.NORMAL, false, true, false));
  }

  @Test void unknownPlatformFailsClosedForEveryNonNormalMode() {
    String oldMode = System.getProperty("canopy.mode");
    String oldAllowed = System.getProperty("canopy.allowSeamless");
    String oldBedrock = System.getProperty("canopy.experimentalBedrockSeamless");
    var player = java.util.UUID.randomUUID();
    try {
      System.setProperty("canopy.mode", "SEAMLESS");
      System.setProperty("canopy.allowSeamless", "true");
      System.setProperty("canopy.experimentalBedrockSeamless", "true");
      assertEquals(Platform.UNKNOWN, platform(player));
      assertEquals(Mode.NORMAL, requested(player));
      assertFalse(mayUseExactHandover(player));

      seamless(player, true);
      assertEquals(Mode.SEAMLESS, sessionMode(player));
      assertEquals(Mode.NORMAL, requested(player));
      assertFalse(mayUseExactHandover(player));

      markPlatform(player, "BROKEN-API-RESULT");
      assertEquals(Platform.UNKNOWN, platform(player));
      assertEquals(Mode.NORMAL, requested(player));
    } finally {
      forget(player);
      restoreProperty("canopy.mode", oldMode);
      restoreProperty("canopy.allowSeamless", oldAllowed);
      restoreProperty("canopy.experimentalBedrockSeamless", oldBedrock);
    }
  }

  @Test void legacyMaintenancePlatformBridgeKeepsBedrockTrialGated() {
    String oldMode = System.getProperty("canopy.mode");
    String oldAllowed = System.getProperty("canopy.allowSeamless");
    String oldBedrock = System.getProperty("canopy.experimentalBedrockSeamless");
    var player = java.util.UUID.randomUUID();
    try {
      System.setProperty("canopy.mode", "SEAMLESS");
      System.clearProperty("canopy.allowSeamless");
      System.clearProperty("canopy.experimentalBedrockSeamless");
      markBedrock(player, true);
      assertEquals(Platform.BEDROCK, platform(player));
      assertEquals(Mode.NORMAL, requested(player));
      System.setProperty("canopy.allowSeamless", "true");
      System.setProperty("canopy.experimentalBedrockSeamless", "true");
      assertEquals(Mode.SEAMLESS, requested(player));

      markBedrock(player, false);
      assertEquals(Platform.BEDROCK, platform(player));
      assertEquals(Mode.SEAMLESS, requested(player));

      markPlatform(player, Platform.JAVA);
      assertEquals(Platform.JAVA, platform(player));
      assertEquals(Mode.SEAMLESS, requested(player));

      var unknown = java.util.UUID.randomUUID();
      markBedrock(unknown, false);
      assertEquals(Platform.UNKNOWN, platform(unknown));
      assertEquals(Mode.NORMAL, requested(unknown));
      forget(unknown);
    } finally {
      forget(player);
      restoreProperty("canopy.mode", oldMode);
      restoreProperty("canopy.allowSeamless", oldAllowed);
      restoreProperty("canopy.experimentalBedrockSeamless", oldBedrock);
    }
  }

  @Test void bedrockRequiresBothTrialFlagsEvenForPlayerOverrides() {
    String oldMode = System.getProperty("canopy.mode");
    String oldNoRespawn = System.getProperty("canopy.noRespawn");
    String oldSeamless = System.getProperty("canopy.seamless");
    String oldAllowed = System.getProperty("canopy.allowSeamless");
    String oldBedrock = System.getProperty("canopy.experimentalBedrockSeamless");
    var player = java.util.UUID.randomUUID();
    try {
      System.clearProperty("canopy.mode");
      System.setProperty("canopy.noRespawn", "false");
      System.setProperty("canopy.seamless", "false");
      System.clearProperty("canopy.allowSeamless");
      System.clearProperty("canopy.experimentalBedrockSeamless");
      markPlatform(player, Platform.BEDROCK);
      seamless(player, true);
      assertEquals(Mode.NORMAL, requested(player));

      System.setProperty("canopy.experimentalBedrockSeamless", "true");
      assertEquals(Mode.NORMAL, requested(player));
      System.setProperty("canopy.allowSeamless", "true");
      assertEquals(Mode.SEAMLESS, requested(player));
      assertFalse(mayUseExactHandover(player));

      System.clearProperty("canopy.experimentalBedrockSeamless");
      assertEquals(Mode.NORMAL, requested(player));
      markPlatform(player, Platform.JAVA);
      assertEquals(Mode.SEAMLESS, requested(player));
      assertTrue(mayUseExactHandover(player));
      forget(player);
      assertEquals(Platform.UNKNOWN, platform(player));
      assertNull(override(player));
    } finally {
      forget(player);
      restoreProperty("canopy.mode", oldMode);
      restoreProperty("canopy.noRespawn", oldNoRespawn);
      restoreProperty("canopy.seamless", oldSeamless);
      restoreProperty("canopy.allowSeamless", oldAllowed);
      restoreProperty("canopy.experimentalBedrockSeamless", oldBedrock);
    }
  }

  @Test void bedrockFastModeFallsBackToNormalUntilBothTrialFlagsAreSet() {
    String oldMode = System.getProperty("canopy.mode");
    String oldAllowed = System.getProperty("canopy.allowSeamless");
    String oldBedrock = System.getProperty("canopy.experimentalBedrockSeamless");
    var bedrock = java.util.UUID.randomUUID();
    var javaPlayer = java.util.UUID.randomUUID();
    try {
      System.setProperty("canopy.mode", "FAST");
      System.clearProperty("canopy.allowSeamless");
      System.clearProperty("canopy.experimentalBedrockSeamless");
      markPlatform(bedrock, Platform.BEDROCK);
      markPlatform(javaPlayer, Platform.JAVA);
      assertEquals(Mode.NORMAL, requested(bedrock));
      assertEquals(Mode.FAST, requested(javaPlayer));

      System.setProperty("canopy.allowSeamless", "true");
      assertEquals(Mode.NORMAL, requested(bedrock));
      System.setProperty("canopy.experimentalBedrockSeamless", "true");
      assertEquals(Mode.FAST, requested(bedrock));
    } finally {
      forget(bedrock); forget(javaPlayer);
      restoreProperty("canopy.mode", oldMode);
      restoreProperty("canopy.allowSeamless", oldAllowed);
      restoreProperty("canopy.experimentalBedrockSeamless", oldBedrock);
    }
  }

  @Test void bedrockCannotUseExactCutEvenWhenSeamlessTrialIsAllowed() {
    String oldMode = System.getProperty("canopy.mode");
    String oldAllowed = System.getProperty("canopy.allowSeamless");
    String oldBedrock = System.getProperty("canopy.experimentalBedrockSeamless");
    var bedrock = java.util.UUID.randomUUID();
    var javaPlayer = java.util.UUID.randomUUID();
    try {
      System.setProperty("canopy.mode", "SEAMLESS");
      System.setProperty("canopy.allowSeamless", "true");
      System.setProperty("canopy.experimentalBedrockSeamless", "true");
      markPlatform(bedrock, Platform.BEDROCK);
      markPlatform(javaPlayer, Platform.JAVA);
      assertEquals(Mode.SEAMLESS, requested(bedrock));
      assertFalse(mayUseExactHandover(bedrock));
      assertTrue(mayUseExactHandover(javaPlayer));
    } finally {
      forget(bedrock); forget(javaPlayer);
      restoreProperty("canopy.mode", oldMode);
      restoreProperty("canopy.allowSeamless", oldAllowed);
      restoreProperty("canopy.experimentalBedrockSeamless", oldBedrock);
    }
  }

  @Test void globalModeApiChangesDefaultsWithoutReplacingPlayerOverrides() {
    String oldMode = System.getProperty("canopy.mode");
    String oldNoRespawn = System.getProperty("canopy.noRespawn");
    String oldSeamless = System.getProperty("canopy.seamless");
    var player = java.util.UUID.randomUUID();
    try {
      markPlatform(player, Platform.JAVA);
      System.clearProperty("canopy.mode");
      System.setProperty("canopy.noRespawn", "false");
      System.setProperty("canopy.seamless", "false");
      assertFalse(globalSeamless());
      assertEquals(Mode.NORMAL, requested(player));

      seamless(player, false);
      setGlobalSeamless(true);
      assertTrue(globalSeamless());
      assertEquals(Mode.NORMAL, requested(player));

      seamless(player, true);
      setGlobalSeamless(false);
      assertFalse(globalSeamless());
      assertEquals(Mode.SEAMLESS, requested(player));
      assertEquals("NORMAL", System.getProperty("canopy.mode"));
    } finally {
      forget(player);
      restoreProperty("canopy.mode", oldMode);
      restoreProperty("canopy.noRespawn", oldNoRespawn);
      restoreProperty("canopy.seamless", oldSeamless);
    }
  }

  @Test void missingOrMalformedFingerprintsCannotSkipConfiguration() {
    assertFalse(sameConfiguration("", ""));
    assertFalse(sameConfiguration("same", "same"));
    assertFalse(sameConfiguration(null, "a".repeat(64)));
    assertFalse(sameConfiguration("a".repeat(64), "b".repeat(64)));
    assertTrue(sameConfiguration("a".repeat(64), "A".repeat(64)));
  }

  private static void restoreProperty(String property, String value) {
    if (value == null) System.clearProperty(property); else System.setProperty(property, value);
  }
}
