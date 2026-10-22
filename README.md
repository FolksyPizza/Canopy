# Canopy

Canopy lets one Minecraft world run on more than one Paper server. You split the world between servers, and players
walk from one to the next without ever seeing a loading screen.

It's experimental. Expect config formats, wire formats and saved data to change between versions.

See also: [Session Gateway](session-gateway/README.md), [seamless switching](docs/seamless-switch.md),
[exact handover](docs/exact-handover.md), [on-demand updates](docs/on-demand-updates.md),
[distribution and availability](docs/distribution-and-availability.md) and the [changelog](CHANGELOG.md).

## Why

A Paper server runs its whole world in one process, so a handful of busy chunks can slow everyone down. Folia helps
by ticking regions in parallel, but the world still lives in a single JVM. Canopy goes a step further and spreads
the world across separate servers, and eventually separate machines, while trying to make the split invisible to
players.

## How it works

Each server (a shard) owns part of the world. Two shards meet at a seam. When a player walks across it, the shard
they're leaving packs up their state (position, inventory, ender chest, effects, health, experience, flight and
momentum), sends it to the other shard, and asks the gateway to move their connection.

The gateway is a modified Velocity proxy. A normal proxy shows a "reconfiguring" or "loading terrain" screen when
it switches you to another server. The Canopy Session Gateway can skip those, so crossing the seam feels like
walking.

A few other things help the two halves feel like one world. Block changes near the seam are copied to the other
side, and day, night and weather stay in sync. If the other shard is down, the player is pushed back with a short
message instead of being dropped.

Optionally, shards can keep player snapshots in MariaDB (we call these managed sessions), so a player's session
carries over between backends and survives a crash from the last save. Both the gateway and the shards also write a
structured audit log. It never includes chat, packet contents or passwords.

## Where things stand

The basics work: two shards split along one X coordinate, with the gateway switching players between them. NORMAL
and FAST switching work. SEAMLESS switching is supported on protocols 771 to 777 (1.21.6 to 26.3), but we're
still checking that every client and server combination behaves.

Managed sessions are experimental and off by default. Exact handover, which cuts input at the precise moment of the
switch, is disabled outside isolated tests. The update coordinator isn't wired in yet. More than two shards, and
moving the boundary while the server runs, aren't built yet.

## What's in this repo

- `src/` is the shard plugin for Paper and Folia. It owns a part of the world, talks to its neighbour over gRPC and
  hands players across the seam.
- `velocity-plugin/` is CanopySwitch, a small plugin on the gateway that does the switch when a shard asks for it.
- `session-gateway/` is the gateway itself: our changes to Velocity, as a patch against a pinned revision. Its
  README explains how to build it.

## Building

You'll need JDK 21 and Maven for the plugins, and Java 25 for the gateway.

```sh
./dev/canopy-dev check
./dev/canopy-dev test
```

`test` builds and tests everything. For the gateway it checks out the pinned Velocity revision in a temporary folder
and applies our patch. When it's done it prints a SHA-256 for each jar. It doesn't start any servers. If your JDKs
aren't on your `PATH`, set `CANOPY_JDK21_HOME` and `CANOPY_JDK25_HOME`. `./dev/canopy-dev --help` lists the probe
and audit tools too.

## Running two shards

1. Put `canopy-*.jar` in each backend's `plugins/` folder, and `canopy-switch-*.jar` in the gateway's.
2. Turn on modern forwarding in the gateway and register both backends.
3. In each backend's `plugins/Canopy/config.yml`, give it its own `shard.id`, point `shard.peers` at the other
   shard, and fill in the `transfer` section (where the boundary is, which side it owns, and the peer's server
   name). Every option is explained in that file.
4. Start the gateway, for example with `-Dcanopy.mode=SEAMLESS`. Seamless switching needs both backends configured
   the same way. If it can't switch a player seamlessly it refuses, unless you've set a fallback.

To make the seam line up, use the same seed and world generation settings on both servers, pre-generate the area
around the seam on both in the same order, and give them the same world border.

In game, `/region` tells you which shard you're on and where the seam is, `/region list` shows the shards, and
`/region go` (operators only) sends you across.

Managed sessions, audit settings and the gateway's control endpoints are set in
`plugins/canopyswitch/session.properties` on the gateway and under `session` in each shard's `config.yml`. Keep the
control traffic on a private network or put TLS in front of it.

## Known gaps

Only blocks are copied across the seam, not chest contents or lighting. Seamless switching doesn't reset every
per-world client state, so scoreboards and teams can carry over. Not every client and server version combination has
been tested, and you can't move the boundary while the world is running.

## License

Canopy is proprietary, and all rights are reserved. See [LICENSE](LICENSE) and [NOTICE](NOTICE). If you use a
published release, its release terms apply.

The exception is `session-gateway/`. It's a modification of [Velocity](https://github.com/PaperMC/Velocity), so it
stays under the GNU General Public License, version 3 (see `session-gateway/LICENSE`). Third-party libraries keep
their own licenses.

Canopy isn't an official Minecraft product and isn't approved by or associated with Mojang or Microsoft.
