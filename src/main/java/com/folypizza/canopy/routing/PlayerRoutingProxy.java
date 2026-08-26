package com.folypizza.canopy.routing;

import com.folypizza.canopy.leader.PartitionMap;
import com.folypizza.canopy.model.TilePosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PlayerRoutingProxy {
    private static final Logger log = LoggerFactory.getLogger(PlayerRoutingProxy.class);
    private static final int REROUTE_THRESHOLD = 1;

    public record PlayerRoute(
        UUID playerId,
        long targetShardId,
        String address,
        int port,
        String world,
        int lastTileX,
        int lastTileZ
    ) {}

    private final ConcurrentHashMap<UUID, PlayerRoute> playerRoutes = new ConcurrentHashMap<>();
    private final PartitionMap partitionMap;
    private final Map<Long, ShardInfo> shardInfoMap = new ConcurrentHashMap<>();

    public record ShardInfo(String address, int port, boolean healthy) {}

    public PlayerRoutingProxy(PartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }

    public long findShardForPosition(double x, double z, String world) {
        TilePosition tile = TilePosition.fromBlockCoords((int) x, (int) z);
        return partitionMap.getState().findShardForTileOrUnowned(tile);
    }

    public PlayerRoute routePlayer(UUID playerId, double x, double z, String world) {
        long targetShard = findShardForPosition(x, z, world);
        var shardInfo = shardInfoMap.get(targetShard);
        if (shardInfo == null) {
            log.warn("No shard info for shard {}", targetShard);
            return null;
        }
        var tile = TilePosition.fromBlockCoords((int) x, (int) z);
        var route = new PlayerRoute(playerId, targetShard, shardInfo.address(), shardInfo.port(), world, tile.x(), tile.z());
        playerRoutes.put(playerId, route);
        log.info("Routed player {} to shard {} at {}", playerId, targetShard, shardInfo.address());
        return route;
    }

    public void updatePlayerPosition(UUID playerId, double x, double z, String world) {
        if (playerRoutes.containsKey(playerId)) {
            long currentShard = findShardForPosition(x, z, world);
            var route = playerRoutes.get(playerId);
            var tile = TilePosition.fromBlockCoords((int) x, (int) z);
            var updated = new PlayerRoute(playerId, currentShard, route.address(), route.port(), route.world(), tile.x(), tile.z());
            playerRoutes.put(playerId, updated);
        }
    }

    public boolean shouldReroute(UUID playerId) {
        var route = playerRoutes.get(playerId);
        if (route == null) return false;
        return false;
    }

    public List<Long> getHealthyShardList() {
        return shardInfoMap.entrySet().stream()
            .filter(e -> e.getValue().healthy())
            .map(Map.Entry::getKey)
            .collect(java.util.stream.Collectors.toList());
    }

    public Optional<String> resolveShardAddress(long shardId) {
        var info = shardInfoMap.get(shardId);
        if (info == null || !info.healthy()) return Optional.empty();
        return Optional.of(info.address() + ":" + info.port());
    }

    public void unroutePlayer(UUID playerId) {
        playerRoutes.remove(playerId);
    }

    public void registerShard(long shardId, String address, int port, boolean healthy) {
        shardInfoMap.put(shardId, new ShardInfo(address, port, healthy));
    }

    public int getActiveRoutes() {
        return playerRoutes.size();
    }
}
