# Seamless server switch

Crossing a seam means changing backend shards. A stock proxy makes that visible twice: the
**reconfiguring** screen and the **loading-terrain** screen. Both have to be removed in the
Session Gateway — a backend plugin cannot change the client's protocol phase or suppress
the world reload. This note describes how each screen arises and how the gateway removes it.
The code lives in `session-gateway/`.

## Reconfiguring screen

Since Minecraft 1.20.2 a server switch sends the client back through the configuration
phase so the new server can deliver its registries. That phase *is* the reconfiguring
screen.

**Removed by `-Dcanopy.seamless=true`.** Backends are assumed identical — same version,
registries, data packs, and feature flags — so the client already holds the new server's
registries and the client-side config phase is redundant.

- The client is kept in the play phase across the switch; `switchToConfigState()` is not
  called, so no screen appears.
- The new backend connection still runs its own login and config on the gateway side, but
  that data is not forwarded to the client.
- `ConfigSessionHandler` echoes the known-packs negotiation back to the backend, because a
  client that stays in the play phase never answers it; without the echo the backend
  handshake stalls.
- When the backend reaches the play phase and sends JoinGame, the gateway completes the
  switch (see below).

## Loading-terrain screen

Even without the config phase, the older fast-switch path sends a fresh JoinGame followed
by a Respawn. That tears down and rebuilds the client's world, which is the loading-terrain
screen. Velocity does this deliberately: a fresh JoinGame lets the client adopt the new
backend's entity ID for the player, so no entity-ID rewriting is needed.

**Removed by `-Dcanopy.noRespawn=true`.** The gateway sends neither JoinGame nor Respawn.
The client keeps its world; the new backend's chunks and entities simply stream in. Two
things a Respawn would otherwise have handled are done explicitly.

### Entity-ID translation

The client stays bound to the entity ID it received in its first JoinGame, but each backend
allocates entity IDs independently. Left alone, the new backend can assign the client's
locked ID to another entity, colliding with the player and freezing them.

The gateway applies one bijection to every entity-ID field in every packet, both
directions:

```
f(id) = clientSelfId    if id == backendSelfId
        backendSelfId   if id == clientSelfId
        id              otherwise
```

`f` is its own inverse, so the same function serves clientbound and serverbound traffic. It
moves the player's own entity onto the ID the client is locked to and relocates whatever
entity held that ID onto the now-free slot, so no collision is possible; all other IDs pass
through untouched. This keeps knockback, riding, sprint/sneak, damage, potion effects, and
equipment working across a switch. The packet-ID and field tables are those of protocol 774
(Minecraft 1.21.11); on any other protocol the gateway falls back to the respawn swap.

### Stale entities

Without a Respawn the client never clears the previous backend's entities, and their IDs
could collide with the new backend's. The gateway tracks every entity a backend spawns and,
on a switch, despawns them all in one Remove-Entities packet before the new backend's spawns
arrive.

## Constraints

- Backends must match. Any registry difference makes the skipped config phase wrong and can
  desync the client.
- The no-respawn path is tied to the protocol version of the pinned gateway build.
- Neither path can be fully verified without a live client, so each change needs an in-game
  pass.

## Out of scope

Stitching two backends into one client view at the same time — a seam with no reload at all
and both sides simulated together — is a much larger change: the gateway would have to merge
chunk and entity streams from both backends. This note covers only the single-backend
switch.
