package com.folksypizza.canopy.session;

import com.folksypizza.canopy.session.PlayerSession.AuthorityToken;
import com.folksypizza.canopy.session.PlayerSession.GatewayToken;
import com.folksypizza.canopy.session.PlayerSession.Phase;

import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

/**
 * Fences state changes by gateway session, backend epoch and snapshot sequence.
 * Calls perform storage I/O and belong on bounded I/O workers, never connection or region threads.
 * A conflict is explicit: callers must re-read and reconcile, not retry an old snapshot blindly.
 * Tokens identify authority; the transport must authenticate the gateway or shard separately.
 */
public final class SessionCoordinator {
    private final SessionStore store;
    private final SessionAudit audit;

    public SessionCoordinator(SessionStore store) { this(store, SessionAudit.NONE); }
    public SessionCoordinator(SessionStore store, SessionAudit audit) {
        this.store = Objects.requireNonNull(store); this.audit = Objects.requireNonNull(audit);
    }

    /** Only CSG opens sessions. Reconnect waits for the prior logout to finish. */
    public PlayerSession open(UUID player, UUID session, String gateway) throws SQLException {
        PlayerSession old = store.find(player).orElse(null);
        if (old != null && old.sessionId().equals(session) && old.gatewayId().equals(gateway) && old.online()) {
            return old;
        }
        require(old == null || old.phase() == Phase.OFFLINE, "Prior session is not finalized");
        // Reusing a closed session ID would allow delayed commands from that session to act again.
        require(old == null || !old.sessionId().equals(session), "Closed session ID cannot be reused");
        PlayerSession next = new PlayerSession(player, session, gateway, Phase.CONNECTED,
            null, null, null, old == null ? 1 : old.epoch() + 1,
            old == null ? 1 : old.revision() + 1, 0, old == null ? new byte[0] : old.snapshot(),
            old == null ? 0 : old.lifeRevision(), old == null ? PlayerSession.Life.ALIVE : old.life());
        require(old == null ? store.insert(next) : store.compareAndSet(old.revision(), next),
            "Concurrent session open");
        audit.state("session.open", next);
        return next;
    }

    /** Destination restores the returned snapshot before serving gameplay. */
    public PlayerSession attach(GatewayToken gateway, String owner) throws SQLException {
        PlayerSession old = current(gateway);
        PlayerSession.requireName(owner);
        if (old.phase() == Phase.ACTIVE && owner.equals(old.owner())) return old;
        require(old.phase() == Phase.CONNECTED, "Initial backend already attached");
        return save("session.attach", old, Phase.ACTIVE, owner, null, null, old.epoch(), 0, old.snapshot());
    }

    public PlayerSession checkpoint(AuthorityToken authority, long sequence, byte[] snapshot) throws SQLException {
        PlayerSession old = owned(authority);
        require(old.phase() == Phase.ACTIVE, "Backend is not allowed to write gameplay state");
        requireSnapshot(old, sequence, snapshot);
        return save("snapshot.checkpoint", old, old.phase(), old.owner(), null, old.transferId(), old.epoch(), sequence, snapshot);
    }

    /** Death and respawn advance authority too, fencing already captured saves from the previous life. */
    public PlayerSession death(AuthorityToken authority, long sequence, byte[] snapshot) throws SQLException {
        PlayerSession old = owned(authority);
        require(old.phase() == Phase.ACTIVE && old.life() == PlayerSession.Life.ALIVE, "Life is not active/alive");
        requireSnapshot(old, sequence, snapshot);
        return saveLife(old, PlayerSession.Life.DEAD, snapshot);
    }

    public PlayerSession respawn(AuthorityToken authority, long expectedLifeRevision, long sequence,
                                byte[] snapshot) throws SQLException {
        PlayerSession old = owned(authority);
        require(old.phase() == Phase.ACTIVE && old.life() == PlayerSession.Life.DEAD
            && old.lifeRevision() == expectedLifeRevision, "Death has been superseded");
        requireSnapshot(old, sequence, snapshot);
        return saveLife(old, PlayerSession.Life.ALIVE, snapshot);
    }

    private PlayerSession saveLife(PlayerSession old, PlayerSession.Life life, byte[] snapshot) throws SQLException {
        PlayerSession next = new PlayerSession(old.playerId(), old.sessionId(), old.gatewayId(), old.phase(),
            old.owner(), null, old.transferId(), old.epoch() + 1, old.revision() + 1, 0, snapshot,
            old.lifeRevision() + 1, life);
        require(store.compareAndSet(old.revision(), next), "Concurrent life transition");
        audit.state(life == PlayerSession.Life.DEAD ? "life.death" : "life.respawn", next);
        return next;
    }

    /** Freeze the source at its input cut, then persist this snapshot before promoting the target. */
    public PlayerSession prepareHandover(AuthorityToken authority, UUID transfer, String target,
                                         long sequence, byte[] snapshot) throws SQLException {
        PlayerSession old = owned(authority);
        Objects.requireNonNull(transfer);
        PlayerSession.requireName(target);
        require(old.phase() == Phase.ACTIVE, "Handoff already pending or session disconnected");
        require(!target.equals(old.owner()), "Handoff target is the current owner");
        require(!transfer.equals(old.transferId()), "Transfer ID cannot be reused");
        requireSnapshot(old, sequence, snapshot);
        return save("handoff.prepare", old, Phase.HANDOFF, old.owner(), target, transfer, old.epoch(), sequence, snapshot);
    }

    /** Only CSG commits, after destination state application has been acknowledged. */
    public PlayerSession commitHandover(GatewayToken gateway, UUID transfer, long sourceEpoch) throws SQLException {
        return commitHandover(gateway, transfer, sourceEpoch, null);
    }

    /** Atomically records the destination's applied envelope with its promotion. */
    public PlayerSession commitHandover(GatewayToken gateway, UUID transfer, long sourceEpoch,
                                       byte[] appliedSnapshot) throws SQLException {
        PlayerSession old = current(gateway);
        require(old.phase() == Phase.HANDOFF && transfer.equals(old.transferId()) && old.epoch() == sourceEpoch,
            "Handoff no longer belongs to this operation");
        byte[] snapshot = appliedSnapshot == null ? old.snapshot() : appliedSnapshot;
        if (snapshot.length == 0 || snapshot.length > PlayerSession.MAX_SNAPSHOT_BYTES)
            throw new IllegalArgumentException("Invalid applied snapshot");
        return save("handoff.commit", old, Phase.ACTIVE, old.target(), null, transfer, old.epoch() + 1, 0, snapshot);
    }

    /** Cancellation also advances the epoch so delayed source writes cannot win after resumption. */
    public PlayerSession cancelHandover(GatewayToken gateway, UUID transfer, long sourceEpoch) throws SQLException {
        PlayerSession old = current(gateway);
        require(old.phase() == Phase.HANDOFF && transfer.equals(old.transferId()) && old.epoch() == sourceEpoch,
            "Handoff no longer belongs to this operation");
        return save("handoff.cancel", old, Phase.ACTIVE, old.owner(), null, transfer, old.epoch() + 1, 0, old.snapshot());
    }

    public PlayerSession cancelCut(AuthorityToken authority, UUID operation) throws SQLException {
        PlayerSession old = owned(authority);
        require(old.phase() == Phase.ACTIVE, "Cut is no longer active");
        return save("handoff.cancel_cut", old, Phase.ACTIVE, old.owner(), null, operation, old.epoch() + 1, 0, old.snapshot());
    }

    /** Only loss of the CSG connection changes online presence; backend quit never calls this. */
    public PlayerSession disconnect(GatewayToken gateway) throws SQLException {
        PlayerSession old = current(gateway);
        if (!old.online()) return old;
        return save("session.disconnect", old, Phase.CLOSING, old.owner(), old.target(), old.transferId(),
            old.epoch(), old.snapshotSequence(), old.snapshot());
    }

    /** Persist the owner's final snapshot and mark offline in the same atomic update. */
    public PlayerSession finishLogout(AuthorityToken authority, long sequence, byte[] snapshot) throws SQLException {
        PlayerSession old = owned(authority);
        require(old.phase() == Phase.CLOSING && old.target() == null,
            "Final save requires a disconnected, unfrozen gameplay owner");
        requireSnapshot(old, sequence, snapshot);
        return save("session.logout", old, Phase.OFFLINE, null, null, old.transferId(), old.epoch() + 1, sequence, snapshot);
    }

    /**
     * Explicit recovery when there is no final owner snapshot (backend crash or disconnected handoff).
     * Retains the last durable checkpoint; never claims unsaved state was recovered.
     */
    public PlayerSession finishFromCheckpoint(GatewayToken gateway) throws SQLException {
        PlayerSession old = current(gateway);
        if (old.phase() == Phase.OFFLINE) return old;
        require(old.phase() == Phase.CLOSING, "Gateway must disconnect before recovery finalization");
        return save("session.recover_checkpoint", old, Phase.OFFLINE, null, null, old.transferId(), old.epoch() + 1,
            old.snapshotSequence(), old.snapshot());
    }

    private PlayerSession current(GatewayToken gateway) throws SQLException {
        PlayerSession old = store.find(gateway.playerId()).orElseThrow(() -> new Conflict("Unknown player session"));
        require(old.sessionId().equals(gateway.sessionId()) && old.gatewayId().equals(gateway.gatewayId()),
            "Gateway session has been superseded");
        return old;
    }

    private PlayerSession owned(AuthorityToken authority) throws SQLException {
        PlayerSession old = current(authority.gateway());
        require(authority.owner().equals(old.owner()) && authority.epoch() == old.epoch(),
            "Backend authority has been superseded");
        return old;
    }

    private static void requireSnapshot(PlayerSession old, long sequence, byte[] snapshot) {
        require(sequence > old.snapshotSequence(), "Snapshot sequence is stale");
        if (snapshot == null || snapshot.length == 0 || snapshot.length > PlayerSession.MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("Invalid snapshot size");
        }
    }

    private PlayerSession save(String event, PlayerSession old, Phase phase, String owner, String target, UUID transfer,
                               long epoch, long sequence, byte[] snapshot) throws SQLException {
        PlayerSession next = new PlayerSession(old.playerId(), old.sessionId(), old.gatewayId(), phase,
            owner, target, transfer, epoch, old.revision() + 1, sequence, snapshot, old.lifeRevision(), old.life());
        require(store.compareAndSet(old.revision(), next), "Concurrent session change; reconcile current authority");
        audit.state(event, next);
        return next;
    }

    private static void require(boolean valid, String message) {
        if (!valid) throw new Conflict(message);
    }

    public static final class Conflict extends IllegalStateException {
        public Conflict(String message) { super(message); }
    }
}
