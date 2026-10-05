# Exact handover

**Status: experimental and disabled by default.** The current implementation does not serialize the cut against every
decoded or queued client packet. If an exact cut has begun, its own timeout still does not prove recovery or release all
gateway-held input. Do not enable it on a live network. The ordinary projected handover remains the active path.

The intended result is a peer arrival with the same position, held slot, health, inventory and motion, without a client
correction. That result has not been established end-to-end for the current implementation.

## Why the projected handover drifts

The projected handover snapshots the player at the crossing and pushes it to the peer, then asks the gateway to
switch. The switch takes a backend login and join (seconds when the peer is cold), and the player keeps moving on the
source shard meanwhile. The peer only has the crossing snapshot, so it projects the player forward by the elapsed time
and teleports them there. The projection is a guess: the client may be somewhere else, and the correction can appear as
a jump, especially when the peer is cold.

## Phases

**Ghost (approach).** While a player heads for the seam within `transfer.ghost.distance`, the source sends
`PrepareArrival` hints to the peer every `interval-ms`. The peer keeps plugin chunk tickets in `preload-radius`
around the predicted landing, so the arrival does not wait on chunk loading or generation. Hints create no player and
change no game state; if the player turns back they stop, and the tickets are released after `expire-ms`.

**Cut (crossing).** The source asks the gateway to switch with the exact flag. The seam-crossing `CanopyHandover` path
still gates only raw unknown packets; decoded packet handlers and already queued work are not covered by one serialized
cut. Therefore the marker does not currently prove a complete input boundary. The separate `/canopyfreeze` maintenance
gate runs before both typed and raw PLAY dispatch, but it is not used by this crossing protocol and does not make its
snapshot complete. The source snapshots at the marker, pushes state to the peer and freezes its copy, but that snapshot
may not include every client action.

**Arrival.** The client switches with the no-respawn switch, so its world stays in place. The peer applies the state
server-side; the gateway can hide its position corrections and replay held raw input after `canopy:ready`. This path
has not been qualified with every packet type or interrupted-transfer case.

## The client's view

The intended client experience keeps one continuous world while authority moves between shards. The current prototype
has not established that result across every client packet or interrupted transfer.

The gateway keeps a bounded chunk view (`-Dcanopy.chunkView.maxBytes`, default 16 MiB) and has unit-tested paths for
reusing identical chunks, sending selected block deltas, and replacing dirty or incompatible chunks. An evicted chunk
is sent whole. These tests do not establish client-visible correctness across every protocol or a full handover.

## Abuse guards

A crossing costs the peer a backend join, a state push and chunk traffic, and every ghost hint and relayed pearl costs
the peer work. The limits come from a profile (`transfer.guard.profile`), and any individual key overrides it:

| Profile | Crossings a minute | Apart | In flight | Peer tick limit | Hints a second | Prepared | Pearl cooldown | Pearls a second |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| off | unlimited | none | unlimited | off | unlimited | 256 | none | unlimited |
| relaxed (default) | 20 | 750 ms | 32 | 60 ms | 500 | 128 | 250 ms | 60 |
| balanced | 8 | 2 s | 8 | 45 ms | 200 | 64 | 1 s | 20 |
| strict | 4 | 4 s | 4 | 40 ms | 100 | 32 | 2 s | 10 |

What each limit covers: the crossing budget (arrivals count too) stops running back and forth across the seam; the
in-flight cap spreads out a crowd crossing at once; the peer tick limit holds crossings into an overloaded peer; the
hint rate and prepared cap stop a crowd at the seam from flooding the peer with chunk loads (a hint never generates
terrain); the pearl limits stop relay spam. These apply in every profile: pushes over 1 MiB are refused and unclaimed
states expire after 30 s; the gateway's chunk view has a per-player budget; the held input and the cut have timeouts.
An optional shared secret (`grpc.shared-secret`) authenticates shard-to-shard calls; without one a shard warns at
startup.

Beyond a limit the seam holds the player on their side (a soft wall with a short notice) instead of handing them
over; nobody is disconnected.

## Shadow-session prototype

An opt-in prototype can open a second backend connection and overlay destination chunks. The backend does not yet
implement a verified hidden/inert ghost lifecycle, and the destination snapshot acknowledgement has no sender in this
tree. Promotion now requires a one-time acknowledgement from the active source connection with the matching random
transfer ID; an acknowledgement timeout closes the shadow and cancels or releases the held cut. Since no trusted
readiness sender exists, this prototype cannot currently promote. Keep both `canopy.shadow` and
`canopy.shadow.experimental` disabled; it is not a zero-downtime update system or safe for live promotion.

## Prototype protocol

| Channel | Direction | Meaning |
| --- | --- | --- |
| `canopy:switch` | shard to gateway | Target server (UTF), then one byte: 1 requests the experimental handover; exact requests also carry a transfer UUID |
| `canopy:cut` | gateway to source shard | Input cut marker: snapshot now |
| `canopy:cut-cancel` | gateway to source shard | The switch failed; resume the player |
| `canopy:ready` | destination shard to gateway | State applied: replay the held input |
| `PrepareArrival` (gRPC) | source shard to peer | Ghost hint: predicted landing |

The prototype holds selected raw movement, player-input and held-slot packets while allowing protocol acknowledgements
through. Its gate does not cover every decoded handler or queued packet, so the input boundary and treatment of other
actions are not yet reliable. The independent maintenance freeze gate does cover dispatch-time input, but it has only
been exercised as an operator pause and does not supply the exact crossing's cut marker, full snapshot, or rollback
coordination. These channels describe the prototype; their presence does not make it safe to enable.

## Failure handling

- A gateway without the exact handover never sends the marker: after one second the source falls back to the
  projected handover. A shard without it never sets the flag, and the gateway does not cut.
- No ready signal within three seconds of arrival (ten of the cut): the exact handover can still hold gameplay input until a ready signal or cancellation arrives. It does not infer that state was applied or fully recover every interrupted cut; the player can remain stuck if destination restoration failed. The source shard only self-thaws after twelve seconds when the player never left it. Shadow promotion has a separate three-second timeout that now aborts and releases/cancels its pending cut instead of promoting.
- A switch into a different world or dimension type uses the respawn switch; held movement is dropped.
- A frozen source copy whose player never leaves resumes after twelve seconds.

## Isolated test configuration

An isolated fixture can be configured with protocol 774 (1.21.11), `transfer.mode: proxy`, `transfer.buffer: 0`, and
`transfer.exact-handover: true` on both shards. This is configuration guidance, not evidence of a successful full
crossing. Keep exact handover disabled on live networks until packet ordering and every timeout, cancellation, and
disconnect path release the player safely. The CanopySwitch plugin registers `canopy:ready` so backends can send it.

## Validation status

Unit tests cover selected protocol packet IDs, chunk-view transformations, serving order, and source/target/transfer
correlation for the shadow promotion gate. They do not exercise the exact cut, all decoded input paths, a real
destination acknowledgement, full timeout recovery, Grim interaction or a full native handover. No end-to-end
exact-handover result is currently claimed.
