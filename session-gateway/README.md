# Canopy Session Gateway

The Session Gateway is the seamless entry point players connect to. It is built on the
Velocity protocol — a fork of the Velocity proxy — with a small set of changes that remove
the two switch-time screens a stock proxy shows when a player crosses between shards.

The full proxy source tree is large and is **not** vendored into this repository. This
folder instead holds everything needed to reproduce the fork: the base revision, the
changed source files as a reference overlay, and a single patch.

## Base revision

- Upstream: PaperMC Velocity, `dev/3.0.0` branch (artifact version `3.6.0-SNAPSHOT`).
- Base commit: `843a47e2a38325309cd66133149fc9a984f76bb8`.

Pin exactly this revision. The no-respawn entity-ID rewriting is protocol-specific (see
below); building against a different Velocity revision can change packet IDs and behaviour.

## Reproducing the build

```sh
git clone https://github.com/PaperMC/Velocity.git
cd Velocity
git checkout 843a47e2a38325309cd66133149fc9a984f76bb8
git apply /path/to/session-gateway/canopy-session-gateway.patch
./gradlew :velocity-proxy:shadowJar
# -> proxy/build/libs/velocity-proxy-3.6.0-SNAPSHOT-all.jar
```

The `proxy/…` tree under this folder mirrors the patched files, so you can also copy them
directly onto a matching checkout instead of applying the patch.

## What changed

All paths are under `proxy/src/main/java/com/velocitypowered/proxy/connection/`.

- **`backend/CanopyEntityRewriter.java`** *(new)* — entity-ID translation for the
  no-respawn switch. Maintains the bijection `f(id) = clientSelfId if id == backendSelfId;
  backendSelfId if id == clientSelfId; id otherwise` and applies it to every entity-ID
  field in every packet, both directions, for protocol 774 (Minecraft 1.21.11). Because
  `f` is its own inverse it serves clientbound and serverbound traffic alike. It also
  tracks spawned entities so the previous backend's entities can be despawned on a switch.
- **`backend/BackendPlaySessionHandler.java`** — `handleUnknown` runs clientbound raw
  packets through the rewriter.
- **`client/ClientPlaySessionHandler.java`** — `handleUnknown` runs serverbound raw
  packets through the rewriter; `handleBackendJoinGame` initialises the rewriter on first
  join and, in no-respawn mode, calls `doNoRespawnClientServerSwitch`, which despawns the
  previous backend's entities and re-points the rewriter instead of sending JoinGame/
  Respawn.
- **`backend/ConfigSessionHandler.java`** — for a seamless switch, suppresses the
  client-facing config-phase writes and echoes the known-packs negotiation back to the
  backend so a client already in the play phase does not stall the handshake.
- **`backend/LoginSessionHandler.java`** — for a seamless switch on 1.20.2+, keeps the
  client in the play phase instead of driving it back into configuration.

## Runtime flags

Both are system properties, off by default; set them on the gateway's Java command line.

| Property | Effect |
| --- | --- |
| `-Dcanopy.seamless=true` | Skip the client config phase on a switch — removes the reconfiguring screen. |
| `-Dcanopy.noRespawn=true` | Send no JoinGame/Respawn on a switch — removes the loading-terrain screen. Enables entity-ID translation. Protocol 774 only; other versions fall back to the respawn swap. |

## Notes and limits

- The entity-ID rewriter parses raw buffers defensively: any unexpected buffer is forwarded
  untouched rather than risking a corrupt stream.
- Entity references *inside* metadata payloads are not rewritten (rare, and unlikely to
  collide with the player's own ID).
- Per-world client state a Respawn would reset (scoreboards, teams) is not reset on a
  no-respawn switch; identical backends avoid this in practice.
- Changes cannot be fully verified without a live client; each change needs an in-game pass.
