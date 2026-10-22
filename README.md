# Canopy

Canopy runs one Minecraft world across several Paper servers and moves players between them without a loading screen.

[Session Gateway](session-gateway/README.md) · [Seamless switching](docs/seamless-switch.md) ·
[Exact handover](docs/exact-handover.md) · [On-demand updates](docs/on-demand-updates.md) ·
[Distribution and availability](docs/distribution-and-availability.md) · [Changelog](CHANGELOG.md)

> **Status: experimental.** Interfaces, wire formats and on-disk layouts can change between versions.

## Why

A Paper server runs a world in one process, and a few busy chunks can hold up every tick. Folia runs regions in
parallel, but the world still lives in one JVM. Canopy splits the world across processes, and later machines, and
keeps the boundary out of the player's way.

## What it does

- **Shards.** Each Paper server owns part of the world. Neighbouring shards meet at a seam.
- **Handover.** A player who crosses the seam is moved to the next shard with their full state: position, inventory,
  ender chest, effects, health, experience, flight and momentum.
- **Session Gateway.** A Velocity-based proxy that switches the player's backend without the reconfiguring or
  loading-terrain screens a stock proxy shows.
- **Seam mirroring.** Block edits near the seam are copied to the neighbour, so each side sees the other.
- **Shared sky.** Day, night and weather stay in step across shards.
- **Managed sessions** (opt-in). Player snapshots and ownership are kept in MariaDB, so a session survives a backend
  switch and a crash is recovered from the last saved state.
- **Audit log.** Structured incident records for lifecycle, saves, failures and switches, without packet contents,
  chat or credentials.

## Status

| Feature | Status |
| --- | --- |
| Two shards split at one X boundary | Experimental |
| Session Gateway, NORMAL and FAST switching | Working |
| Session Gateway, SEAMLESS switching | Protocols 771 to 777 (1.21.6 to 26.3); parity still being verified |
| Managed sessions | Experimental, opt-in |
| Exact handover (input cut) | Disabled by default; isolated tests only |
| Update coordinator | Not yet integrated |
| More than two shards, repartitioning while running | Not implemented |

## Components

| Path | What it is |
| --- | --- |
| `src/` | The Canopy shard plugin for Paper and Folia: owns a part of the world, coordinates with its peers over gRPC and hands players across the seam. |
| `velocity-plugin/` | CanopySwitch, loaded on the Session Gateway: performs the switch when a shard asks for a handover. |
| `session-gateway/` | The Canopy Session Gateway: a modification of Velocity with FAST and SEAMLESS switching. Its README gives the base revision and the patch. |

## Build

The shard plugin and CanopySwitch need JDK 21 and Maven. The Session Gateway needs Java 25.

```sh
./dev/canopy-dev check
./dev/canopy-dev test
```

`test` builds and tests everything, including the gateway (it applies the patch to the pinned Velocity revision in a
temporary checkout), and prints the SHA-256 of each jar. It starts no server. Point it at your JDKs with
`CANOPY_JDK21_HOME` and `CANOPY_JDK25_HOME` if they are not on your `PATH`. Run `./dev/canopy-dev --help` for the
probe and audit tools.

## Run a two-shard world

1. Put `canopy-*.jar` in each backend's `plugins/` and `canopy-switch-*.jar` in the gateway's `plugins/`.
2. Set up modern forwarding on the gateway and register both backends.
3. In each backend's `plugins/Canopy/config.yml`, set a unique `shard.id`, the peer's address under `shard.peers`,
   and the `transfer` section (boundary, side owned, peer server). Every option is documented in the file.
4. Start the gateway, for example with `-Dcanopy.mode=SEAMLESS`. SEAMLESS needs matching backend configurations;
   anything it cannot switch falls back only if you configure a fallback.

For the seam to look like one world, give both backends the same seed and generation settings, pre-generate the
shared area on both in the same order, and set the same world border.

`/region` shows which shard you are on and where the seam is; `/region list` lists the shards; `/region go`
(operators) moves you to the peer.

Managed sessions, audit settings and the gateway's control endpoints are configured in
`plugins/canopyswitch/session.properties` on the gateway and under `session` in each shard's `config.yml`. Keep
control traffic on a private network or behind TLS.

## Limitations

- Seam mirroring copies blocks only, not block-entity contents or lighting.
- SEAMLESS switching does not reset every per-world client state, such as scoreboards and teams.
- Client and backend version combinations are not all verified.
- Repartitioning a running world is future work.

## License

Canopy is proprietary. All rights reserved; see [LICENSE](LICENSE) and [NOTICE](NOTICE). Use of published releases
is governed by the terms released with them.

`session-gateway/` contains modifications of [Velocity](https://github.com/PaperMC/Velocity) and stays under the
GNU General Public License, version 3 (`session-gateway/LICENSE`). Third-party libraries keep their own licenses.

Not an official Minecraft product. Not approved by or associated with Mojang or Microsoft.
