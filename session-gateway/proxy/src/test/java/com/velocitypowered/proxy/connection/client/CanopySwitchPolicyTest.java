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

  @Test void globalModeApiChangesDefaultsWithoutReplacingPlayerOverrides() {
    String oldMode = System.getProperty("canopy.mode");
    String oldNoRespawn = System.getProperty("canopy.noRespawn");
    String oldSeamless = System.getProperty("canopy.seamless");
    var player = java.util.UUID.randomUUID();
    try {
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
