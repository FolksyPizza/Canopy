# Canopy

Canopy runs one Minecraft world across several Paper/Folia server processes. Each process,
called a shard, owns a contiguous slice of the world, and the slices meet at a seam.
Players cross between shards through a Velocity proxy, and neighboring shards exchange
state over gRPC so the seam reads as one continuous world.

Status: experimental prototype. APIs and on-disk formats are not stable.

## Components

`src/` is the Canopy plugin, a Paper/Folia plugin that owns a world slice, tracks tile
versions and per-region metrics, coordinates with peer shards over gRPC, and hands players
across the seam.

`velocity-plugin/` is CanopySwitch, a small Velocity plugin that carries out the
proxy-side server switch when a shard requests a handover.

## How it works

Partitioning: a shard owns one side of an X boundary. The band
`[boundary-buffer, boundary+buffer)` is left inaccessible. A player who reaches it is
handed to the peer shard and lands at a fixed point on the far edge, so the shards do not
overlap and the handover point stays put.

Handover: when a player reaches the buffer, the shard serializes their state (position,
gamemode, flight, vitals, XP, inventory, ender chest), pushes it to the destination shard
over gRPC, and asks the proxy to switch the player. The destination applies the state on
join.

Unavailable peer: if the destination shard is not reachable, the crossing is refused. The
player stays in their region and gets a short notice instead of being dropped into a dead
connection.

Seam mirroring: each shard records block edits in a strip beside the seam. The peer pulls
them over gRPC and applies them to its own copy, so recent changes on the far side show up
from across the seam. Base terrain already matches because both shards use the same world
seed.

Coordination: shards find peers from config, poll each other's health, and serve
tile-version and migration data over gRPC. Redis-backed lease coordination is optional,
with an in-memory implementation used otherwise.

## Building

Requires JDK 21 and Maven.

    mvn package                              # Canopy plugin -> target/canopy-*.jar
    mvn -f velocity-plugin/pom.xml package   # CanopySwitch  -> velocity-plugin/target/canopy-switch-*.jar

## Running

A minimal cluster is two Paper/Folia backends behind one Velocity proxy.

1. Put `canopy-*.jar` in each backend's `plugins/`, and `canopy-switch-*.jar` in the
   proxy's `plugins/`.
2. Configure Velocity modern forwarding and register the two backends (for example `west`
   and `east`).
3. In each backend's `plugins/Canopy/config.yml`, set a unique `shard.id`, the peer's gRPC
   address in `shard.peers`, and the `transfer` section (boundary, buffer, `owns`,
   `peer-server`, ports).

Every option is documented inline in `src/main/resources/config.yml`.

## Limitations

- Seam mirroring covers block state only, not tile-entity contents or lighting.
- Player-state sync does not yet include potion effects.
- Runtime repartitioning is not implemented. Changing which shard owns a region while the
  cluster runs, including merging region files between processes, is future work.
