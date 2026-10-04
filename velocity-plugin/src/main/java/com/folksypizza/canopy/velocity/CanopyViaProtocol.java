package com.folksypizza.canopy.velocity;

import com.viaversion.viaversion.api.Via;
import com.viaversion.viaversion.api.connection.UserConnection;
import com.viaversion.viaversion.api.protocol.version.ProtocolVersion;
import com.viaversion.viaversion.api.protocol.version.VersionProvider;
import org.slf4j.Logger;

/** Via translates client packets into this protocol before CSG decodes them. */
final class CanopyViaProtocol implements VersionProvider, AutoCloseable {
    private final ProtocolVersion internal;
    private final Logger logger;
    private VersionProvider previous;

    CanopyViaProtocol(int protocol, Logger logger) {
        if (protocol < 771 || protocol > 777 || !ProtocolVersion.isRegistered(protocol)
            || !com.velocitypowered.api.network.ProtocolVersion.isSupported(protocol)) {
            throw new IllegalArgumentException("CSG Via internal protocol must have a supported modern profile");
        }
        this.internal = ProtocolVersion.getProtocol(protocol);
        this.logger = logger;
    }

    void install() {
        if (Via.getManager().isInitialized()) {
            activate();
        } else {
            Via.getManager().addPostEnableListener(this::activate);
        }
    }

    private void activate() {
        var providers = Via.getManager().getProviders();
        previous = providers.get(VersionProvider.class);
        if (previous == null || previous == this) {
            throw new IllegalStateException("ViaVersion has no original protocol provider");
        }
        providers.use(VersionProvider.class, this);
        logger.info("CSG Via translation uses internal protocol {} ({})", internal.getVersion(), internal.getName());
    }

    @Override public ProtocolVersion getClosestServerProtocol(UserConnection connection) throws Exception {
        // Backend connections still use Via's actual destination-version detection.
        return connection.isClientSide() ? previous.getClosestServerProtocol(connection) : internal;
    }

    @Override public ProtocolVersion getClientProtocol(UserConnection connection) {
        return previous.getClientProtocol(connection);
    }

    @Override public void close() {
        if (previous != null && Via.getManager().getProviders().get(VersionProvider.class) == this) {
            Via.getManager().getProviders().use(VersionProvider.class, previous);
        }
    }
}
