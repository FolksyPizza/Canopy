# On-demand zero-downtime updates

This is a design proposal, not an implemented update pipeline. The standby process should be created only for an update
window, so it does not consume steady-state CPU and memory. A second server must remain inert until it owns the world;
running two active copies would duplicate ticks and side effects.

## Proposed flow

1. **Prepare on demand.** Start a standby from the target code and a consistent durable checkpoint while the current
   process remains authoritative. The update controller records a deployment ID and source authority epoch.
2. **Catch up.** Replay a durable, ordered world-change journal to the standby. Every entry needs a sequence number,
   idempotency key and owner epoch. The current Canopy code has no complete world-mutation journal or replay engine.
3. **Freeze at the final barrier.** CSG stops gameplay input, drains in-flight actions and takes a final barrier
   snapshot. Show the player one green system-chat notice, `WARNING: The Server Is Restarting`; keep the same client
   session and append the notice to chat without clearing prior messages. The snapshot watermark must include player
   inventories and effects, entities, block entities, scheduled ticks, world time and other state the target process
   will own.
4. **Prove readiness.** The standby applies through the barrier watermark, restores each player and completes an
   authoritative tick. It acknowledges the exact deployment ID, world identity, sequence and player-state revisions.
   It stays non-authoritative until this acknowledgement is durable. Missing or mismatched acknowledgement aborts the
   update before promotion; return players to the source and unfreeze them there.
5. **Fence and promote.** Atomically advance the authority epoch so the old process cannot write after promotion. Route
   the existing CSG sessions to the new process, then unfreeze them once the destination readiness acknowledgement is
   confirmed. Verify movement, interactions, saves and audit continuity. A short hold after transfer is acceptable;
   releasing input before the new owner is ready is not.
6. **Recover or retire.** Before the authority fence, failures return players to the old owner. After the fence, the
   former process must stay unable to write; keep affected players held for explicit recovery instead of reactivating
   stale state. On success, stop the former process or leave it idle as the next update target. Do not keep it
   simulating the world after promotion.

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

Canopy now contains `ZeroDowntimeUpdateCoordinator`, `WorldUpdateState`, `WorldAuthority`, and
`JdbcWorldUpdateStore`. This unintegrated core persists per-world update phases, reserves one active deployment per
world, checks a final replay watermark and continuity evidence, atomically advances the authority epoch, and separates
recovery before and after that fence. Focused unit tests exercise the coordinator against an in-memory store.

No live backend calls this coordinator. Canopy still has no crash-consistent world checkpoint provider, complete ordered
world-mutation journal, deterministic replay engine, standby boot/catch-up lifecycle, or global world-write enforcement.
An isolated Vanilla fixture controller has exercised the gateway's `/canopyfreeze` gate during a held backend restart,
but that controller does not call `ZeroDowntimeUpdateCoordinator` or provide complete-world replay. A synthetic
Bedrock protocol-776 session stayed connected through backend stop/copy/start, return, chat and movement. A second run
forced candidate plugin readiness to fail; the controller started the rollback build and returned the same held session.
Neither run emitted additional Bedrock `start_game`, respawn, dimension-change or transfer packets. This verifies one
Geyser-translated protocol path and two isolated recovery outcomes; it does not verify authenticated devices, every
protocol, production parity or a continuously replicated world.

The coordinator capability values are evidence required for promotion; no runtime provider produces or independently
verifies them yet. Player snapshots alone cannot satisfy those gates, and shadow chunk streaming is not a world journal.
Canopy therefore has no generic end-to-end update system.

The next implementation work is to connect trusted providers for the checkpoint, journal, replay and world authority
fence, then integrate the controller with the coordinator and gateway sessions. Promotion must remain unavailable until
the source process can prove it has stopped writing and the standby has applied the exact final watermark. The tested
held restart is an isolated adapter path, not the generic coordinator's promotion flow.

## Replacing the gateway process

An [NGINX stream listener](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html) can proxy TCP and UDP and
select an upstream for newly accepted traffic. It does not move an already-established Minecraft TCP stream to
another Canopy process, nor does it recreate that process's protocol
handlers, encryption/compression state, packet queues or backend attachment. Switching the upstream therefore
helps direct new connections to a standby, but players already attached to the old gateway remain attached to it.

Bedrock has a separate UDP path: [Geyser](https://geysermc.org/wiki/geyser/setup/) terminates Bedrock transport and
translates the session into Java protocol traffic. Putting a UDP forwarder in front of Geyser can steer new Bedrock
sessions; it does not transfer an existing Geyser/RakNet session or its Canopy state to a second proxy. This is an
architectural inference from the current Geyser/Velocity topology and the stream proxy's transport-level behavior,
not a tested failover result.

The practical first target is to keep one Canopy session gateway alive while starting the update standby on demand,
then move backend simulation ownership only after the checkpoint, ordered journal, final input barrier and authority
fence have passed their gates. This can update the game shards without spending resources on a permanent standby.
Updating Canopy or Geyser itself while preserving already-connected players requires a separate persistent session
endpoint with a defined way to transfer or replicate the complete Java and Bedrock connection state; a generic
TCP/UDP load balancer is not that endpoint. Until that exists, a gateway binary restart can preserve new-login
availability through a second instance but cannot promise uninterrupted sessions for connected players.

## Fully continuous proxy-code updates

For a connected player to see no update at all, the process that owns the client's live connection must stay alive.
The current Velocity gateway owns the Java TCP stream, including encryption, compression, packet ordering and the
backend attachment. [Geyser listens for Bedrock over UDP](https://geysermc.org/wiki/geyser/setup/) and owns the
translated session state as well. Moving only the listener to a second process cannot recreate those live sessions.

The target architecture is a stable, versioned session host with replaceable Canopy decision and control modules.
The host keeps the Java and Bedrock transports, protocol codecs, client-visible state and ordered packet queues. A
new Canopy module starts beside the old one on demand, restores a compatible state snapshot, consumes a sequenced
event journal, and first runs in shadow mode without producing side effects. Once it reports the same session and
policy state at a safe event-loop barrier, the host changes the module generation for each session, drains old
in-flight work, and fences the old generation from writes. If validation fails before the fence, the host continues
with the old generation. This lets the standby use resources only during an update window.

This boundary must include all state that Canopy code currently keeps inside Velocity connection handlers. A module
must not own a socket, encryption/compression context, packet sequence, Geyser connection, RakNet reliability state,
or mutable player session object that the host cannot preserve. The current source overlay changes Velocity core
connection classes, so it has not yet established this hot-swappable boundary. Updating those core classes still
requires a process replacement or a much harder full connection-state migration. Velocity's
[`/velocity reload`](https://docs.papermc.io/velocity/built-in-commands/) rereads `velocity.toml`; it does not
replace its running code.

## Player-visible continuity contract

- Keep each Java TCP and Bedrock UDP session attached to the same live session host. Do not ask the client to log in
  again, and do not send a disconnect, configuration reset, JoinGame, Respawn, or chat-clear packet as part of an
  update.
- Keep the client's visible chat scrollback by retaining the same client session. Preserve server-side chat history
  and audit streams as append-only state with event IDs so a module transition neither erases nor duplicates messages.
- Preserve world, position, view, inventory, selected slot, effects, entities, scoreboard and teams, tab list,
  boss bars, titles, resource-pack state, command tree, permissions, anticheat context, and any active Bedrock forms,
  camera state or emotes. If a specific subsystem needs refresh packets, prove that they do not reset unrelated client
  state and do not replay prior chat.
- Keep vanilla/backend-instance promotion as a separate barrier protocol. A stable proxy session does not make a
  second world simulation safe: the destination still needs a consistent checkpoint, complete ordered world journal,
  final input barrier, durable authority fence and rollback rules before it may tick.

The alpha must be described as seamless only for the exact session host and module boundary it verifies. A two-proxy
[NGINX stream](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html) or HAProxy setup can drain new logins to a replacement while old sessions stay on the old process, but it is a
rolling availability strategy, not a no-disconnect upgrade for everyone. A future update to the stable session host
itself needs either a proven in-process module boundary that leaves transport state untouched or complete, ordered
Java and Bedrock connection-state migration; this repository has not implemented either guarantee.
