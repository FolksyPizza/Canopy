package com.folksypizza.canopy.update;

import java.util.Objects;
import java.util.UUID;

/** Durable authority for one world, independent of per-player session ownership. */
public record WorldAuthority(String worldId, String ownerInstance, long epoch, UUID deploymentId) {
    public WorldAuthority {
        requireName(worldId, "worldId");
        requireName(ownerInstance, "ownerInstance");
        if (epoch < 1) throw new IllegalArgumentException("Invalid world authority epoch");
    }

    private static void requireName(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.length() > 128) throw new IllegalArgumentException("Invalid " + field);
    }
}
