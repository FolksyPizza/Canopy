# Canopy Session Gateway

CSG owns the player's client connection, continuous world view and ordered input routing.
Canopy shards compute authoritative gameplay for the regions and players they own.
This directory contains the source overlay, protocol tables and reproducible patch.

## Build

The base is PaperMC Velocity `dev/4.0.0`, artifact `4.2.1-SNAPSHOT`, at
`7fc49913145fc6195db95995eb622bf9f5592876`. The revision is also recorded in
`velocity-revision.txt`. Building and running this gateway requires Java 25.

```sh
git clone https://github.com/PaperMC/Velocity.git
cd Velocity
git checkout 7fc49913145fc6195db95995eb622bf9f5592876
git apply /path/to/session-gateway/canopy-session-gateway.patch
./gradlew :velocity-proxy:test --tests '*Canopy*' :velocity-proxy:shadowJar
```

The patch includes client state resets; an additional no-respawn patch is unnecessary.

## Switch modes

Select a mode with `-Dcanopy.mode=NORMAL|FAST|SEAMLESS`. The default is NORMAL.
Initial login always performs configuration and JoinGame.

| Mode | Subsequent backend switch |
| --- | --- |
| NORMAL | Full configuration and upstream JoinGame/Respawn transition. |
| FAST | Skip redundant configuration; retain the upstream JoinGame/Respawn transition. |
| SEAMLESS | Skip redundant configuration, preserve the client world, translate backend entity IDs and refresh backend state. No switch-time JoinGame or Respawn. |

FAST and SEAMLESS require matching deployment fingerprints on the source and destination:
`-Dcanopy.configurationFingerprint.<backend>=<64-hex-digest>`.
These are operator assertions, not hashes measured by CSG. Generate them from all client-visible
configuration, including registries, tags, enabled features and resource packs. Update them whenever
that configuration changes. Without matching valid fingerprints, FAST performs NORMAL instead.

SEAMLESS rejects unsupported switches by default. `-Dcanopy.modeFallback=FAST` or `NORMAL`
explicitly permits a lower mode when configuration or protocol checks fail before configuration.
A different client world or dimension type rejects the switch before detaching the source.
Vanilla gameplay respawns, such as death, remain normal backend packets.

Legacy `canopy.noRespawn=true` selects SEAMLESS, while `canopy.seamless=true` selects FAST.
An explicit `canopy.mode` takes precedence. The legacy properties no longer bypass compatibility checks.

## Protocols

The pinned upstream recognizes 1.21.6 through 26.3, including patch versions sharing a protocol.
CSG has explicit play-packet ID tables for protocols 771 through 777:
1.21.6; 1.21.7/1.21.8; 1.21.9/1.21.10; 1.21.11; 26.1/26.1.1/26.1.2; 26.2; 26.3.
Unknown future protocols have no translation profile and cannot use SEAMLESS.

The earlier tables use PrismarineJS minecraft-data protocol definitions. The 26.2 and 26.3
IDs come from Mojang's server-generated packet reports. Translation tests cover wire IDs,
self-ID collisions, damage references, newer attack/spectator requests and generated join-state packets.
These tests do not certify every mechanic or every client/server pairing. Mixed server versions
require a compatible protocol translator and matching effective client configuration.

For protocols newer than 1.21.11, changed chunks are sent whole. Identical clean chunks may still
be reused. Block/light delta codecs for these newer layouts need separate verification.

## Client version translation

Modern shards can accept older clients through an optional Via integration in CanopySwitch. Install
[ViaVersion](https://github.com/ViaVersion/ViaVersion/wiki/Installation) and
[ViaBackwards](https://github.com/ViaVersion/ViaBackwards) on the gateway;
[ViaRewind](https://github.com/ViaVersion/ViaRewind) supplies the 1.8 translation path.
Do not also install the translators on backends in this topology.

Set `-Dcanopy.via.protocol=774` for an internal 1.21.11 protocol. Supported internal profiles are
771 through 777, and the running gateway and Via builds must both recognize the selection.
The adapter selects the frontend's internal protocol before CSG packet decoding. CSG chunk caching,
entity translation and handoff operate on that modern protocol; Via translates outbound packets into
the client's wire version. Backend-side Via detection continues to use the actual destination version.
Without this property the adapter leaves Via's protocol selection unchanged.

This integration uses ViaVersion's public API, with an optional plugin dependency. Matching effective
backend configuration is still required for SEAMLESS. Installing translation plugins does not establish
seamless compatibility for every version or backend pair; test the actual translator/internal/backend
combination. Missing ViaVersion or an invalid internal selection produces an initialization error.

Clients before 1.17 cannot represent modern expanded world height. Item/block substitutions and some
menu or interaction limitations are inherent in older client protocols; see ViaBackwards' limitations.
A 1.8 client connecting to a modern backend does not acquire modern client capabilities.

## Parallel serving

Chunk work runs on a shared worker pool, with an ordered lane per player shared by source and shadow
connections. The next task waits for the preceding computation and network delivery. Different
players can compute concurrently. Network writes and backend read control stay on their connection
loops; saturation retries asynchronously rather than running chunk computation on a network thread.

| Property | Default |
| --- | --- |
| `canopy.serving.enabled` | `true` |
| `canopy.serving.threads` | Half the available processors, bounded to 2 through 8 |
| `canopy.serving.readyCapacity` | `512` |
| `canopy.chunkView.maxBytes` | `16777216` per player |
| `canopy.exactHandover` | `false`; test only until packet-cut and cancellation coverage is complete |
| `canopy.shadow` | `false` |

Exact handover and shadow login/promotion remain experimental and are off by default. Exact handover
has not yet proven that every decoded and queued client packet is included in the cut or that an
interrupted cut can always recover. Shadow promotion lacks a verified destination snapshot
acknowledgement path and native ghost lifecycle. Isolated shadow tests require both
`-Dcanopy.shadow=true` and `-Dcanopy.shadow.experimental=true`; managed sessions disable them.

## Stale backend death recovery

An incoming alive snapshot can repair a dead player copy left on the destination shard. The backend
decodes the snapshot before native respawn, uses its arrival coordinates and then restores current
progress. At an alive exact cut, the source pre-arms recovery on the next held attachment. This lets a
dead destination respawn before its entity scheduler retires. Restored arrivals wait briefly for the
gateway's readiness channel before releasing held input.

CSG hides one explicitly marked same-world repair during that destination's held arrival. It retains
the client chunk view, stages health/XP/slot updates until readiness, and acknowledges the hidden native
load reset. Genuine death and ordinary respawn retain their normal packets. Dead or incomplete snapshots
are refused; this recovery cannot determine which unfenced legacy snapshot is globally current.

Handoff timeout recovery is incomplete. Some paths leave input held after source-side recovery, and release across all
gateway/backend cancellation and disconnect paths has not been verified. Managed database sessions add death
revisions, logout/reconnect fencing and dead-attachment routing for native bed/anchor respawns; they do not yet provide
a verified recovery guarantee for every interrupted activation. Complete native timer/state restoration also remains
unfinished. See the root README for opt-in lifecycle configuration.

## Remaining verification

Packet translation does not yet cover all entity references inside metadata or spawn object data.
Native player-state parity, owner-linked mechanics, failure recovery and the complete version matrix
need gameplay tests. The independent CSG session bridge is present and opt-in; shared database/control
configuration is required. The default remains `session.enabled: false`. Native 1.21.11 lifecycle probes do
not certify the complete version matrix or all vanilla mechanics. No production deployment is implied by
a successful build.

### Incident audit

The matching CanopySwitch plugin installs the native audit sink before players enter play. Gateway and
managed backend plugin folders contain private rotating `audit/events.jsonl` files. The sink records
mode admission, input cut/replay/cancel/overflow, arrival/repair, serving failures, backend loss and
shadow transitions alongside the managed session/control journal. `audit.packet-trace=true` in the
plugin's `session.properties` adds raw play-packet IDs/sizes and generic decoded packet types at the
CSG processing hooks, excluding payloads. This is incident metadata, not a complete wire capture or
gameplay replay. See the root README for retention, queue limits and failure semantics.
