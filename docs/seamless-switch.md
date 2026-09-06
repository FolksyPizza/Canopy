# Seamless server switch

When a player crosses a seam they change backend shards. The goal is for the client to
show a short chunk reload, the same as a dimension change, instead of Velocity's
"reconfiguring" screen. This has to be done in the proxy; a backend plugin cannot change
the client's protocol phase.

## Why the screen shows up

Since Minecraft 1.20.2 a server switch sends the client back through the configuration
phase so the new server can deliver its registries. That phase is the reconfiguring
screen.

The relevant code is in the Velocity proxy under
`proxy/src/main/java/com/velocitypowered/proxy/connection`:

- `client/ConnectedPlayer.switchToConfigState()` writes `StartUpdatePacket` to the
  client, which moves it into the configuration phase.
- That call comes from `backend/BackendPlaySessionHandler.handle(StartUpdatePacket)`
  while a switch is in progress.
- Once the new backend finishes config and sends `JoinGame`,
  `backend/TransitionSessionHandler.handle(JoinGamePacket)` calls
  `client/ClientPlaySessionHandler.handleBackendJoinGame(...)`.

## The path that skips it

`handleBackendJoinGame` already contains a world swap that never touches the config
phase, used for a player who is still in the play phase:

- `doFastClientServerSwitch` sends JoinGame in a different dimension, then a Respawn in
  the right one.
- `doSafeClientServerSwitch` sends a Respawn into a scratch dimension, then into the
  target dimension.

Both reload the world without a config phase. This is the older fast-switch path and it
only runs when the client did not enter the configuration phase during the switch.

## Plan

Backends are assumed to be configured identically: same version, registries, data packs,
and feature flags. Operators are responsible for that. Given it, the client already holds
the new server's registries, so the client-side config phase is redundant and can be
skipped.

For a switch marked "seamless":

1. Do not call `switchToConfigState()`. The client stays in the play phase, so no screen.
2. Let the new backend connection run its own login and config on the proxy side. That
   data is not forwarded to the client, because the client is not in the config phase.
3. When the backend reaches play and sends `JoinGame`, `TransitionSessionHandler` runs
   `handleBackendJoinGame`, which does the Respawn swap.

The player sees a chunk reload and no screen.

## Changes in the fork

- Gate `ConnectedPlayer.switchToConfigState()` and the `StartUpdatePacket` handling in
  `BackendPlaySessionHandler` so a seamless switch does not move the client into config.
- Let `backend/ConfigSessionHandler` finish the backend config without transitioning the
  client.
- Carry a "seamless" flag on the connection request. Canopy sets it for hops between
  registered backends; the existing `canopy:switch` message still decides when a hop
  happens.

## Constraints

- Backends must match. Any registry difference makes the skipped config phase wrong and
  can desync the client.
- The change is tied to the protocol version of the pinned Velocity release.
- It cannot be verified without a live client, so each change needs an in-game pass.

## Out of scope

Stitching two backends into one client view at the same time, for a seam with no reload
at all, is a much larger change: the proxy would have to merge chunk and entity streams
from both backends. This document only covers the chunk-reload switch.
