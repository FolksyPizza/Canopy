# Distribution and availability: design draft

> **Status:** First draft for architecture review. The database and multi-gateway capabilities below are future goals, not implemented features.

## Why Canopy exists

Canopy exists to distribute Minecraft simulation across processes and machines while making that distribution
observationally disappear as far as practical. A player should experience the behavior of one ordinary server even
when different shards own different parts of the world and a handoff moves simulation to another process. Preserving
the player's session, world view, movement, chat history and gameplay state is a core product goal, not an optional
polish step.

Canopy therefore has two separate scaling problems. Backend load balancing distributes world simulation. Network
load balancing distributes incoming player connections and packet work across gateway compute. More backend shards do
not increase the network capacity of a single gateway machine.

## Player-data databases

Managed player sessions currently use one shared MariaDB database. Canopy does not yet support multiple database
authorities or automatic per-player database failover.

The target is to let Canopy assign each player's authoritative data to a database group, with one or more configured
replicas for additional resilience. If a database is saturated or unavailable, Canopy should be able to move that
player's write authority to a healthy backup after it has confirmed the latest durable player-data revision. The
system must prevent two databases from accepting conflicting writes as current authority.

Each player's updates should carry a monotonic authority epoch and ordered revision. Replication should be
idempotent, detect missing or conflicting revisions, and expose replica lag and health. A failover should fence the
old writer before the backup accepts writes. If the backup has not reached the required durable revision, Canopy
should hold or reject writes according to an explicit availability policy instead of silently rolling the player back
or allowing split-brain state. Multiple database copies improve resilience only when authority and recovery rules are
clear.

Per-player assignment can spread hot records across databases; replicas can protect those records from a single
database failure. These are related but distinct features. Neither replaces capacity monitoring, connection pooling,
bounded retries or database load testing.

## Distributed Canopy Session Gateways

The Canopy Session Gateway (CSG) owns live Java TCP sessions; Geyser owns the translated Bedrock session and UDP
transport. Scaling backend simulation does not distribute those gateway sockets or their packet-processing load.

The target is to run multiple CSG instances across compute machines, with connection-level network load balancing for
new Java and Bedrock sessions. CSG should also use parallel processing where it is safe, while keeping packets and
state transitions for each player in order. Multithreading one gateway and spreading gateway instances across hosts
solve different limits; both require per-session ownership and bounded work queues.

A network load balancer can choose a gateway for a new connection. An established Minecraft TCP stream or Bedrock UDP
session remains owned by the gateway that accepted it. A generic load balancer cannot move that live transport to a
different process. Keeping an existing player seamlessly attached during gateway failure or code replacement needs a
stable session host or a separately designed transfer of the complete protocol and session state. Until that exists,
gateway balancing should preserve connection affinity and use other healthy gateways for new connections.

## Continuity requirements

The distribution should be as unnoticeable as practical to the player:

- Keep the same client connection through backend ownership changes whenever the selected protocol path supports it.
- Preserve world and player state, movement, entities, inventory, chat scrollback, and the session's visible UI.
- Keep one authoritative writer for each world and player at a time, with a durable, monotonic handoff between owners.
- Measure transfer, database replication and gateway health. When continuity cannot be proven, fail closed and report the
  affected boundary instead of claiming a seamless switch.

The current seamless and managed-session code covers only selected paths. Protocol compatibility, full world-state
replay, database failover, multi-instance CSG, cross-gateway session recovery and zero-downtime gateway-code updates
remain separate qualification goals. See [the seamless-switch notes](seamless-switch.md) and [on-demand update design](on-demand-updates.md)
for current implementation limits.

## Suggested implementation order

1. Measure backend tick and storage load separately from gateway bandwidth, packet rate and event-loop delay.
2. Define player-data ownership, replication watermarks, write durability, failover fencing and recovery behavior.
3. Add replica health and controlled per-player failover, then exercise saturation, lag, database loss and recovery.
4. Make CSG's per-session work safe to process concurrently without reordering packets or state transitions.
5. Add multiple CSG instances and distribute new connections while keeping established sessions on their current owner.
6. Design and qualify session-host failover separately; do not treat network load balancing as migration of a live
   Minecraft session.

These steps are proposals for review. None of the database failover or distributed CSG behavior described here should
be read as currently available.
