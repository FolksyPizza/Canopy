# Canopy

Canopy runs a single Minecraft world across several Paper/Folia server processes. Each
process — a *shard* — owns a contiguous slice of the world, and the slices meet at a
*seam*. Players walk across the seam from one shard to the next through the Canopy Session
Gateway, and neighbouring shards exchange state over gRPC so the seam reads as one
continuous world.

> **Status:** experimental. Interfaces, wire formats, and on-disk layouts are not stable.

## Why

A single Paper server is bound to one process and, in practice, a few busy chunks can
dominate a tick. Folia relaxes this by scheduling independent regions in parallel, but a
world still lives inside one JVM. Canopy takes the next step: it splits the world across
processes (and, eventually, machines) and makes the boundary between them invisible to the
player.

## Components

| Path | What it is |
| --- | --- |
| `src/` | **Canopy shard plugin** — a Paper/Folia plugin that owns a world slice, tracks tile versions and per-region metrics, coordinates with peers over gRPC, and hands players across the seam. |
| `velocity-plugin/` | **CanopySwitch** — a small plugin, loaded on the Session Gateway, that performs the proxy-side server switch when a shard requests a handover on the `canopy:switch` channel. |
| `session-gateway/` | **Canopy Session Gateway** — the changes that turn a stock proxy into a seamless one. Built on the Velocity protocol (a fork of the Velocity proxy); see the folder's README for the base revision, the source overlay, and the patch. |

## How it works

**Partitioning.** A shard owns one side of an X boundary. With a zero-width buffer the seam
is a single line and coordinates are continuous 1:1. A non-zero buffer leaves an
inaccessible band `[boundary − buffer, boundary + buffer)` and begins the landing at its
far edge.

**Handover.** When a player crosses toward the peer, the shard serialises their state —
position, game mode, flight, elytra, active firework boost, potion effects, vitals,
experience, inventory, armour, off-hand, and ender chest — pushes it to the destination
shard over gRPC, and asks the gateway to switch
the connection. The destination projects the crossing position along the player's recent
movement for the time spent switching backends, then restores their view direction and
momentum.

**Exact handover.** An experimental input-cut path exists, but it is disabled by default. It does not yet serialize
the cut against every decoded and queued client packet, and an interrupted arrival can leave input held. The approach
phase preloads chunks; it is not an invisible second gameplay server. Keep exact handover and shadow sessions disabled
outside isolated tests. See `docs/exact-handover.md` for the current limits.

**Ender pearls.** A pearl thrown across the seam continues its flight on the peer shard.
When it lands, the peer reports the impact to the thrower's shard, which hands the player
over to that position. A pearl that lands on the thrower's own shard uses normal server
behaviour.

**Unavailable peer.** If the destination shard is unreachable the crossing is refused: the
player is knocked back into their own region with a short maintenance notice instead of
being dropped into a dead connection.

**Seam mirroring.** Each shard records block edits in a strip beside the seam. The peer
pulls them over gRPC and applies them to its own copy, so recent changes on the far side
are visible from across the seam. Base terrain already matches because both shards share a
world seed.

**Coordination.** Shards discover peers from configuration, poll each other's health, and
serve tile-version and migration data over gRPC. Redis-backed lease coordination is
optional; an in-memory implementation is used otherwise.

**World time and weather.** Day/night and storms are kept in step across the seam. Each
shard reports its overworld time and weather in its health response; the shard with the
lowest id is the authority, and the others follow it, so the sky and weather match on both
sides of the boundary.

## The Canopy Session Gateway

Crossing the seam means changing backend servers. A stock proxy can show a "reconfiguring"
screen while it repeats the client configuration phase and a "loading terrain" screen when
a fresh join or respawn rebuilds the client's world. The Session Gateway provides FAST and
SEAMLESS modes that aim to avoid those transitions; NORMAL remains the default, and seamless
behavior requires a supported protocol profile and matching backend configuration.

Select `-Dcanopy.mode=NORMAL`, `FAST` or `SEAMLESS`. NORMAL uses the full configuration
and JoinGame/Respawn transition. FAST skips redundant configuration. SEAMLESS preserves
the client world without a switch-time JoinGame or Respawn and translates backend-local
entity IDs. Configuration reuse requires matching configured fingerprints; unsupported
SEAMLESS transitions are rejected unless a fallback is explicitly configured.

The current gateway targets protocols 771 through 777 (1.21.6 through 26.3), with
version-specific packet tables. Complete vanilla parity and client/server combinations
remain under verification. See [Session Gateway](session-gateway/README.md) for modes,
compatibility requirements, parallel serving and remaining limitations.

## Managed player lifecycle

Managed sessions are opt-in. The first managed admission imports the selected backend's native player file;
existing networks must select or migrate the authoritative bootstrap copy. CSG opens and closes the logical session through authenticated control
endpoints independent of the player's backend connection. All managed shards share one MariaDB database
and control secret. Backend join/quit during handoff does not mark the player offline.

Managed handoffs persist player snapshots and ownership revisions, but the experimental exact input-cut path does not
yet establish a complete packet barrier. Promotion records the destination's applied snapshot and a new authority
epoch. Death and real respawn advance a separate life revision and the authority epoch;
old saves cannot overwrite the new life. Logout persists the final owner's snapshot before admitting a
replacement session. A stopped backend can finalize only from its last durable checkpoint after lease expiry;
unwritten changes cannot be recovered.

Bed/anchor references retain global world/block coordinates and forced-spawn policy. CSG moves a dead
attachment to the block's owner before forwarding the native respawn request. Vanilla validates the spawn
and consumes the charge there. Broken-spawn fallback retains its resolved position and routes authority to
that position's owner. This genuine respawn is distinct from a hidden stale-death repair during an alive
seamless handoff.

Configure each shard's `session` block, including its gateway server name, database, control bind/port and
shared secret. Supply `owned-chunks` as `world,chunkX,chunkZ` entries and set the same `default-spawn-owner`
on every shard. Entries bootstrap `canopy_chunk_owners`; conflicting owners are rejected. This directory
supports spawn lookup by chunk set. Walking still uses the configured X seam; directory seeding does not
perform repartitioning. World names, dimensions, gamerules and world-spawn settings must agree across shards.

Canopy now has an unintegrated, fail-closed update coordinator with durable world-authority epochs, an atomic promotion
fence, and explicit checkpoint, journal, session, chat, and recovery evidence. No backend currently supplies a complete
world checkpoint or mutation journal to it, and it does not yet boot a standby or route sessions. The proxy also has an
experimental `/canopyfreeze` input gate for operator-managed holds; that gate does not coordinate an update or transfer
world authority. Canopy does not yet provide an end-to-end update system. See [On-demand updates](docs/on-demand-updates.md).

Create `plugins/canopyswitch/session.properties` on CSG:

```properties
enabled=true
gateway-id=gateway-a
directory=alpha
backend.alpha=http://127.0.0.1:50151
backend.beta=http://127.0.0.1:50152
shared-secret=change_me_with_a_random_value
```

`directory` names the preferred managed endpoint; directory calls can use another configured endpoint when
it is unavailable. Owner-specific calls still reach the actual simulation owner. Keep control HTTP on a
trusted private network or use TLS. Deploy matching gateway/plugin builds together; stock Velocity lacks
the lifecycle input hooks. Shadow login is disabled for managed sessions. Full native state, pearl lifecycle,
all failure cases and the client/backend version matrix still need qualification.

## Audit logging

CSG and managed shards write structured incident metadata to `audit/events.jsonl` in their plugin data
folders. Records include UTC time, process run ID, audit sequence, player/session/transfer IDs, authority
and life revisions, control request IDs, outcomes, durations and failure classes. Lifecycle, durable saves,
leases/storage failures, cancellation, protocol selection, input gating, chunk-view handoff summaries,
serving failures and experimental shadow transitions are recorded. UUIDs are retained for correlation;
credentials, packet bodies, inventory contents, chat and exception messages are excluded.

Gateway settings live in `plugins/canopyswitch/session.properties`:

```properties
audit.enabled=true
audit.max-bytes=16777216
audit.retained-files=8
audit.queue-capacity=4096
audit.packet-trace=false
```

Managed shard equivalents live under `session.audit` in `config.yml`. Changes require restart.
Packet trace adds play-packet IDs/types, direction and size; it does not capture payloads. Enable it for
focused incident reproduction: high-volume tracing can fill the bounded queue. The default keeps an
active 16 MiB file plus eight rotated files per process. Files use owner-only permissions on POSIX systems.
Keep these runtime logs private and out of source control.

The writer forces buffered data to disk roughly once per second while healthy and drains for up to five
seconds on graceful shutdown. Queue saturation or disk failure emits a console warning and an `audit.gap`
record when writing recovers. Abrupt process termination can lose pending records; this diagnostic journal
is not a write-ahead transaction log, an exact gameplay replay, or a tamper-proof record.

## Building

Backend and CanopySwitch builds require JDK 21 and Maven. The Session Gateway requires
Java 25 and its own Gradle build.

```sh
mvn install                               # Canopy plugin and shared session classifier -> target/canopy-*.jar
mvn -f velocity-plugin/pom.xml package    # CanopySwitch  -> velocity-plugin/target/*.jar
```

To build the gateway, apply `session-gateway/canopy-session-gateway.patch` onto the base
Velocity revision named in `session-gateway/README.md`, then run its Gradle `shadowJar`
task.

## Running

A minimal cluster is two Paper/Folia backends behind one Session Gateway.

1. Place `canopy-*.jar` in each backend's `plugins/`, and `canopy-switch-*.jar` in the
   gateway's `plugins/`.
2. Configure the gateway's modern forwarding and register the two backends (for example
   `west` and `east`).
3. In each backend's `plugins/Canopy/config.yml`, set a unique `shard.id`, the peer's gRPC
   address in `shard.peers`, and the `transfer` section (boundary, buffer, `owns`,
   `peer-server`, ports).
4. Start the gateway with the switch behaviour you want, for example
   `-Dcanopy.mode=SEAMLESS`, with matching configuration fingerprints on both shards.

Every option is documented inline in `src/main/resources/config.yml`.

### World setup

For the seam to read as one world, both backends must generate the **same terrain**:

- Give every backend the **same `level-seed`** (and the same generation settings). With a
  shared seed the base terrain and vanilla features are deterministic, so the two sides line
  up at the boundary.
- **Pre-generate the shared area** on both backends before players arrive, using the same
  region and order (a pre-generator such as Chunky, centred on the seam with a matching
  radius). On-demand generation at the seam can otherwise place edge features — trees
  spanning a chunk border, for instance — differently depending on load order; pre-generating
  both sides identically avoids that.
- Set a matching world border on both backends so the owned area is bounded the same way.

## In-game administration

`/region` reports the shard you are on, the seam boundary, and which side you own;
`/region list` shows the shards this process knows about; `/region go` (operator only) hands
you to the peer.

## Limitations

- Seam mirroring covers block state only — not tile-entity contents or lighting.
- The gateway has explicit packet profiles for protocols 771–777, but support across all
  client/backend combinations is unverified. The no-respawn switch does not reset every
  per-world client state such as scoreboards and teams; matching backends reduce configuration
  differences but do not prove complete parity.
- Runtime repartitioning is not implemented. Changing which shard owns a region while the
  cluster runs, including merging region files between processes, is future work.
