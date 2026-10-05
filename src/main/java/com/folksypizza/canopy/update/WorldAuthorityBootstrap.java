package com.folksypizza.canopy.update;

import io.papermc.paper.update.WorldAuthorityControl;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/** Fail-closed bootstrap for an already-authorized world owner or a closed standby process. */
public final class WorldAuthorityBootstrap {
    private WorldAuthorityBootstrap() {}

    @FunctionalInterface
    public interface AuthorityControlOpener {
        WorldAuthorityControl open(WorldAuthorityControl.AuthorityProof proof);
    }

    /** The accepted Paper handle and its startup phase; a STANDBY result remains non-authoritative. */
    public record Result(WorldAuthorityControl control, WorldAuthorityControl.Snapshot snapshot) {
        public Result {
            Objects.requireNonNull(control, "control");
            Objects.requireNonNull(snapshot, "snapshot");
        }

        public WorldAuthorityControl.Phase phase() { return snapshot.phase(); }
    }

    /** Reads and validates durable state without opening Paper admission. Call this before starting services. */
    public static WorldAuthorityControl.AuthorityProof prepare(String configuredWorldId, WorldUpdateStore store)
            throws SQLException {
        return readStableProof(configuredWorldId, store);
    }

    /** Revalidates durable state, then lets Paper validate and apply the exact proof as the final startup step. */
    public static Result open(String configuredWorldId, WorldAuthorityControl.AuthorityProof preparedProof,
                              AuthorityControlOpener opener, WorldUpdateStore store) throws SQLException {
        Objects.requireNonNull(preparedProof, "preparedProof");
        Objects.requireNonNull(opener, "opener");
        WorldAuthorityControl.AuthorityProof currentProof = readStableProof(configuredWorldId, store);
        if (!preparedProof.equals(currentProof)) {
            throw new IllegalStateException("Durable world authority changed during Canopy startup");
        }

        WorldAuthorityControl control = opener.open(preparedProof);
        if (control == null) throw new IllegalStateException("Paper world-authority API is unavailable");
        if (control.apiVersion() != WorldAuthorityControl.API_VERSION) {
            throw new IllegalStateException("Unsupported Paper world-authority API version");
        }
        WorldAuthorityControl.Snapshot snapshot = control.snapshot();
        requireAcceptedSnapshot(snapshot, preparedProof);
        return new Result(control, snapshot);
    }

    private static WorldAuthorityControl.AuthorityProof readStableProof(
            String configuredWorldId, WorldUpdateStore store) throws SQLException {
        requireName(configuredWorldId, "Configured world ID");
        Objects.requireNonNull(store, "store");

        if (store.findActiveUpdate(configuredWorldId).isPresent()) {
            throw new IllegalStateException("An unresolved world update prevents authority bootstrap");
        }
        WorldAuthority authority = store.findAuthority(configuredWorldId)
            .orElseThrow(() -> new IllegalStateException("Durable world authority is missing"));
        Optional<WorldUpdateState> completedTransition = requireDurableAuthorityRecord(authority, configuredWorldId, store);

        // Confirm the durable proof did not change while its authority and completion receipt were read.
        if (store.findActiveUpdate(configuredWorldId).isPresent()) {
            throw new IllegalStateException("An unresolved world update prevents authority bootstrap");
        }
        if (!authority.equals(store.findAuthority(configuredWorldId).orElse(null))) {
            throw new IllegalStateException("Durable world authority changed during bootstrap");
        }
        if (!completedTransition.equals(requireDurableAuthorityRecord(authority, configuredWorldId, store))) {
            throw new IllegalStateException("Durable authority update record changed during bootstrap");
        }

        return new WorldAuthorityControl.AuthorityProof(
            configuredWorldId, authority.ownerInstance(), authority.epoch(), authority.deploymentId());
    }

    private static Optional<WorldUpdateState> requireDurableAuthorityRecord(
            WorldAuthority authority, String configuredWorldId, WorldUpdateStore store) throws SQLException {
        if (!configuredWorldId.equals(authority.worldId())) {
            throw new IllegalStateException("Durable authority does not match configured world ID");
        }
        if (authority.deploymentId() == null) return Optional.empty();

        WorldUpdateState transition = store.findUpdate(authority.deploymentId())
            .orElseThrow(() -> new IllegalStateException("Durable authority update record is missing"));
        if (!authority.deploymentId().equals(transition.deploymentId())
                || !configuredWorldId.equals(transition.worldId())
                || !authority.ownerInstance().equals(transition.targetInstance())
                || authority.epoch() != transition.sourceEpoch() + 1
                || transition.phase() != WorldUpdateState.Phase.COMPLETED
                || !transition.hasRequiredPromotionCapabilities()) {
            throw new IllegalStateException("Durable authority does not match a completed update record");
        }
        return Optional.of(transition);
    }

    private static void requireAcceptedSnapshot(
            WorldAuthorityControl.Snapshot snapshot, WorldAuthorityControl.AuthorityProof proof) {
        if (snapshot == null || !proof.worldId().equals(snapshot.worldId())) {
            throw new IllegalStateException("Configured world ID does not match Paper world-authority identity");
        }
        requireName(snapshot.instanceId(), "Paper instance ID");
        if (snapshot.epoch() < 1 || snapshot.activeLeases() != 0 || snapshot.transitionId() != null
                || snapshot.targetInstance() != null) {
            throw new IllegalStateException("Paper world-authority state is not idle");
        }

        if (snapshot.phase() == WorldAuthorityControl.Phase.ACTIVE) {
            if (!proof.ownerInstance().equals(snapshot.instanceId()) || proof.epoch() != snapshot.epoch()) {
                throw new IllegalStateException("Active Paper identity does not match durable authority");
            }
            return;
        }

        // Paper returns STANDBY only for a prebooted ghost one epoch behind a different durable owner.
        if (snapshot.phase() == WorldAuthorityControl.Phase.STANDBY
                && !proof.ownerInstance().equals(snapshot.instanceId())
                && snapshot.epoch() == proof.epoch() + 1) {
            return;
        }
        throw new IllegalStateException("Paper did not accept this process as the active owner or a closed standby");
    }

    private static void requireName(String value, String label) {
        if (value == null || value.isBlank() || value.length() > 128) {
            throw new IllegalStateException(label + " is missing or invalid");
        }
    }
}
