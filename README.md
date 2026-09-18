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
is a single line and coordinates are continuous 1:1 — a player standing on a block steps
across and lands on the same block on the peer. A non-zero buffer instead leaves an
inaccessible band `[boundary − buffer, boundary + buffer)` and lands the player at a fixed
point on the far edge.

**Handover.** When a player crosses toward the peer, the shard serialises their state —
position, game mode, flight, elytra, vitals, experience, inventory, armour, off-hand, and
ender chest — pushes it to the destination shard over gRPC, and asks the gateway to switch
the connection. Position and view direction are captured at the exact moment of crossing and
restored as the player arrives, so they resume from where — and where they were looking —
when they stepped over the seam.

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

Crossing the seam means changing backend servers. On a stock proxy that shows two visible
seams of its own: the "reconfiguring" screen (the client is sent back through the
configuration phase on every switch since Minecraft 1.20.2) and the "loading terrain"
screen (a fresh join/respawn tears down and rebuilds the client's world). The Session
Gateway removes both.

- **Seamless switch** (`-Dcanopy.seamless=true`) keeps the client in the play phase across
  the switch, so the reconfiguring screen never appears. Backends are assumed to be
  configured identically, so the client already holds the registries the config phase would
  have re-sent.
- **No-respawn switch** (`-Dcanopy.noRespawn=true`) goes further and sends neither JoinGame
  nor Respawn, so the client keeps its world and never shows the loading-terrain screen.
  Because the client stays bound to its original entity ID while each backend allocates IDs
  independently, the gateway rewrites entity IDs in both directions so the player's own
  entity — and anything that would otherwise collide with it — stays consistent. This path
  is version-specific and gated to the pinned protocol.

The two flags are independent and off by default; with neither set the gateway behaves like
the upstream proxy. See `docs/seamless-switch.md` for the design and `session-gateway/` for
the code.

## Building

Requires JDK 21 and Maven; the Session Gateway additionally uses its own Gradle build.

```sh
mvn package                               # Canopy plugin -> target/canopy-*.jar
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
   `-Dcanopy.seamless=true -Dcanopy.noRespawn=true`.

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
- The no-respawn switch is tied to a single protocol version and does not reset per-world
  client state such as scoreboards and teams on a switch; identical backends avoid this in
  practice.
- Runtime repartitioning is not implemented. Changing which shard owns a region while the
  cluster runs, including merging region files between processes, is future work.
