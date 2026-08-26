# Canopy

Canopy shards a single Minecraft world across multiple Paper/Folia server processes.
Each process (a *shard*) owns a contiguous slice of the world; the slices meet at a seam.
Players cross between shards through a Velocity proxy, and neighboring shards exchange
state over gRPC so the seam behaves as one continuous world.

Status: experimental prototype. APIs and on-disk formats are not stable.

## Components

- **Canopy plugin** (`src/`) — a Paper/Folia plugin. Owns a world slice, tracks tile
  versions and per-region metrics, coordinates with peer shards over gRPC, and hands
  players across the seam.
- **CanopySwitch** (`velocity-plugin/`) — a small Velocity plugin that performs the
  proxy-side server switch when a shard requests a handover.

## How it works

- **Partitioning.** A shard owns one side of an X boundary. The band
  `[boundary-buffer, boundary+buffer)` is an inaccessible buffer. A player who reaches it
  is handed to the peer shard and lands at a fixed point on the far edge, so there is no
  overlap between shards and the handover point does not drift.
- **Handover.** On reaching the buffer, the shard serializes the player's full state
  (position, gamemode, flight, vitals, XP, inventory, ender chest) and pushes it to the
  destination shard over gRPC, then asks the proxy to switch the player. The destination
  applies the state on join.
- **Seam mirroring.** Each shard records block edits in a strip next to the seam; the peer
  pulls them over gRPC and mirrors them into its own copy, so recent changes on the far
  side are visible from across the seam. Base terrain matches because both shards share a
  world seed.
- **Coordination.** Shards discover peers from config, poll each other's health, and
  expose tile-version and migration services over gRPC. Redis-backed lease coordination is
  optional; an in-memory implementation is used otherwise.

## Building

Requires JDK 21 and Maven.

    mvn package                              # Canopy plugin  -> target/canopy-*.jar
    mvn -f velocity-plugin/pom.xml package   # CanopySwitch    -> velocity-plugin/target/canopy-switch-*.jar

## Running

A minimal cluster is two Paper/Folia backends behind one Velocity proxy.

1. Place `canopy-*.jar` in each backend's `plugins/`, and `canopy-switch-*.jar` in the
   proxy's `plugins/`.
2. Configure Velocity modern forwarding and register the two backends (for example `west`
   and `east`).
3. In each backend's `plugins/Canopy/config.yml`, set a unique `shard.id`, the peer's gRPC
   address in `shard.peers`, and the `transfer` section (boundary, buffer, `owns`,
   `peer-server`, ports).

All options are documented inline in `src/main/resources/config.yml`.

## Limitations

- Seam mirroring covers block state only, not tile-entity contents or lighting.
- Player-state sync does not yet include potion effects.
- Runtime repartitioning — changing which shard owns a region while running, including
  merging region files between processes — is not yet implemented.
