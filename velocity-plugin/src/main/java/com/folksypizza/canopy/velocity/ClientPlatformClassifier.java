package com.folksypizza.canopy.velocity;

import java.util.UUID;

/** Resolves a proxy session's client platform without treating lookup failure as Java. */
final class ClientPlatformClassifier {
    enum Platform { JAVA, BEDROCK, UNKNOWN }

    private ClientPlatformClassifier() { }

    /**
     * A null API result means its lookup was unavailable or failed. Java is only confirmed when both
     * installed platform APIs completed successfully and reported that this is not a Bedrock session.
     */
    static Platform classify(UUID playerId, Boolean floodgatePlayer, Boolean geyserPlayer) {
        if (playerId == null) return Platform.UNKNOWN;
        if (playerId.getMostSignificantBits() == 0) return Platform.BEDROCK;
        if (Boolean.TRUE.equals(floodgatePlayer) || Boolean.TRUE.equals(geyserPlayer)) {
            return Platform.BEDROCK;
        }
        if (Boolean.FALSE.equals(floodgatePlayer) && Boolean.FALSE.equals(geyserPlayer)) {
            return Platform.JAVA;
        }
        return Platform.UNKNOWN;
    }
}
