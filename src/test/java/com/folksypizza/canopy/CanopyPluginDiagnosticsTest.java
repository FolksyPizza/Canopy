package com.folksypizza.canopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class CanopyPluginDiagnosticsTest {

    @Test
    void redisFailureDiagnosticOmitsExceptionMessage() {
        String sensitiveDiagnostic = "sensitive diagnostic content";

        String diagnostic = CanopyPlugin.redisFailureKind(new IllegalArgumentException(sensitiveDiagnostic));

        assertEquals("IllegalArgumentException", diagnostic);
        assertFalse(diagnostic.contains(sensitiveDiagnostic));
    }
}
