package com.folksypizza.canopy.velocity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class ClientPlatformClassifierTest {
    @Test
    void confirmsJavaOnlyWhenBothPlatformLookupsSucceedAndAreNegative() {
        assertEquals(ClientPlatformClassifier.Platform.JAVA,
            ClientPlatformClassifier.classify(UUID.randomUUID(), false, false));
    }

    @Test
    void eitherPositiveLookupConfirmsBedrock() {
        assertEquals(ClientPlatformClassifier.Platform.BEDROCK,
            ClientPlatformClassifier.classify(UUID.randomUUID(), true, false));
        assertEquals(ClientPlatformClassifier.Platform.BEDROCK,
            ClientPlatformClassifier.classify(UUID.randomUUID(), false, true));
        assertEquals(ClientPlatformClassifier.Platform.BEDROCK,
            ClientPlatformClassifier.classify(UUID.randomUUID(), true, null));
    }

    @Test
    void missingOrFailedLookupIsUnknownRatherThanJava() {
        assertEquals(ClientPlatformClassifier.Platform.UNKNOWN,
            ClientPlatformClassifier.classify(UUID.randomUUID(), false, null));
        assertEquals(ClientPlatformClassifier.Platform.UNKNOWN,
            ClientPlatformClassifier.classify(UUID.randomUUID(), null, false));
        assertEquals(ClientPlatformClassifier.Platform.UNKNOWN,
            ClientPlatformClassifier.classify(UUID.randomUUID(), null, null));
        assertEquals(ClientPlatformClassifier.Platform.UNKNOWN,
            ClientPlatformClassifier.classify(null, false, false));
    }

    @Test
    void zeroMostSignificantUuidConfirmsBedrockWhenApisAreUnavailable() {
        assertEquals(ClientPlatformClassifier.Platform.BEDROCK,
            ClientPlatformClassifier.classify(new UUID(0, 42), null, null));
    }
}
