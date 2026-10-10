# Changelog

All notable changes to Canopy are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Canopy is experimental and has no release yet; interfaces,
wire formats and on-disk layouts can change between commits.

## [Unreleased]

### Added
- Canopy Session Gateway as a source overlay and patch on a pinned Velocity revision, with protocol-aware chunk
  handling and managed player sessions. Native Java protocol profiles 771 to 777 are recognized.
- Seamless backend switches are gated per protocol: each native protocol needs its own measured configuration
  approval across every registered backend, and anything not approved, including Bedrock, uses a normal switch.
- Recipient chat counters and native signed chat state carry across seamless backend switches, so chat after a
  switch is not rejected by the client. Signed chat migration needs the companion backend adapter; without it the
  switch keeps the source session and falls back.
- Bedrock (Geyser) handover support: a guarded gameplay freeze during a switch, bounded input buffering, and input
  recovery when the destination fails. Production Bedrock sessions still use normal switches.
- Startup validation of durable world authority, and revalidation of active world updates during recovery.
  Zero-downtime updates are not complete: this covers the coordinator and authority checks only.
- Edits beside the seam in chunks that are not loaded are buffered and applied when the chunk loads.
- Developer tool (`dev/canopy-dev`): one test command, Java and Bedrock status probes, and an audit summary that
  leaves out player-identifying data.
- Design documents for distribution and availability goals and for on-demand updates, including the partial Paper
  authority fencing and the checkpoint requirements still open.

### Changed
- The metrics endpoint honors its configured address and port.
- Gateway backend selection and the client brand shown on the F3 screen are corrected.
- Exact handover and shadow-session promotion stay disabled by default until cancellation and destination lifecycle
  coverage are complete.

### Fixed
- A seamless switch falls back to a normal switch when the source backend's configuration no longer matches its
  approved measurement.
- Redis credentials no longer appear in diagnostics.

## Development history before the session gateway (to 2026-09-26)

### Added
- Canopy shard plugin for Paper and Folia: each shard owns one side of an X boundary, players are handed across the
  seam with their full state, shards coordinate over gRPC, and block edits beside the seam are mirrored, including
  explosions and fluids.
- CanopySwitch, the proxy-side plugin that switches a player's backend when a shard requests a handover.
- Refused crossings to an unreachable shard: the player is moved back into their own region with a maintenance
  notice, and a downed peer is detected within one health-poll cycle.
- Seam containment: mobs, items, hoppers and off-side pearl or chorus teleports cannot cross the seam. `/region`
  reports the current shard and lets operators hand themselves to the peer.
- A no-respawn switch path that keeps the client's world loaded, with entity ID translation between backends.
- World time and weather follow the lowest shard ID across all shards.
- Ender pearls thrown across the seam continue on the peer shard and hand the thrower over where they land.

### Changed
- Crossings capture position and view direction at the exact border and restore them on arrival, project the
  landing forward along the player's movement (`transfer.landing-lead-ticks`), and keep momentum and the selected
  hotbar slot.
- Arrival state is polled every 40 ms instead of 250 ms, and the seam markers are easier to see.
- The default seam has a 4-block inaccessible band with a deterministic landing at its far edge; a zero-width seam
  with 1:1 coordinates is configurable.
