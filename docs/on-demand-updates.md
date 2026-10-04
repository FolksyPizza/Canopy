# On-demand zero-downtime updates

This is a design proposal, not an implemented update pipeline. The standby process should be created only for an update
window, so it does not consume steady-state CPU and memory. A second server must remain inert until it owns the world;
running two active copies would duplicate ticks and side effects.

## Proposed flow

1. **Prepare on demand.** Start a standby from the target code and a consistent durable checkpoint while the current
   process remains authoritative. The update controller records a deployment ID and source authority epoch.
2. **Catch up.** Replay a durable, ordered world-change journal to the standby. Every entry needs a sequence number,
   idempotency key and owner epoch. The current Canopy code has no complete world-mutation journal or replay engine.
3. **Cut input.** CSG holds new player input, drains in-flight actions and takes a final barrier snapshot. The snapshot
   watermark must include player inventories and effects, entities, block entities, scheduled ticks, world time and
   other state the target process will own.
4. **Prove readiness.** The standby applies through the barrier watermark and acknowledges the exact deployment ID,
   world identity, sequence and player-state revisions. It stays non-authoritative until this acknowledgement is
   durable. Missing or mismatched acknowledgement aborts the update and releases input back to the source.
5. **Fence and promote.** Atomically advance the authority epoch so the old process cannot write after promotion. Route
   the existing CSG sessions to the new process, then verify movement, interactions, saves and audit continuity.
6. **Retire on success.** Stop the former process or leave it idle as the next update target. Do not keep it simulating
   the world after promotion.

## Correctness gates

- A checkpoint must be crash-consistent while the active process continues writing; copying live region files is not a
  valid snapshot protocol.
- Journal coverage must include all world mutations and order-dependent simulation state, not only player snapshots or
  seam chunks. Replay must preserve vanilla tick order and random sequences.
- Promotion needs a single durable authority fence, duplicate suppression and rollback behavior for failures before and
  after that fence. After the fence, rollback must not reactivate stale state.
- The old process must not tick entities, redstone, scheduled block/fluid work or plugin callbacks while it is a
  standby. A proxy-only hidden player connection does not provide this guarantee.
- Qualification must include interrupted boot, partial replay, missing acknowledgement, crash during promotion,
  reconnect, death/respawn, anticheat and supported Java/Bedrock client matrices.

## Current state

Canopy managed sessions persist player state and ownership epochs; they do not replicate a complete running world or
launch a standby process. The proxy shadow prototype opens a second per-player backend connection and streams selected
chunk changes; it is not a ghost server and is not safe for promotion. There is no scheduler, world journal, checkpoint
catch-up or rollback pipeline yet.

The first implementation should add an update controller and verifiable checkpoint/journal contract behind a disabled
feature flag. It should not attempt live world copying until every mutation source has an ordered, tested replay path.
