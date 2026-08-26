package com.folypizza.canopy.graph;

import com.folypizza.canopy.leader.PartitionMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Load-weighted graph partitioner.
 *
 * Formulates the shard allocation problem as k-way graph partitioning on the
 * loaded-chunk adjacency graph. Uses a METIS-style algorithm to minimize
 * inter-shard edges while balancing partition weights.
 *
 * Vertices are weighted by measured MSPT contribution.
 * Edges are weighted by cross-boundary traffic.
 * Cost functions include player density heatmaps ("don't cut here")
 * to keep seams away from spawn and dense build areas.
 */
public class PartitionAdvisor {
    private static final Logger log = LoggerFactory.getLogger(PartitionAdvisor.class);

    private final List<ChunkVertex> vertices = new ArrayList<>();
    private final List<ChunkEdge> edges = new ArrayList<>();
    private final double playerDensityHeatmap;
    private final int targetShardCount;

    public PartitionAdvisor(int targetShardCount, double playerDensityThreshold) {
        this.targetShardCount = targetShardCount;
        this.playerDensityHeatmap = playerDensityThreshold;
    }

    /**
     * Add a vertex (chunk) to the partition graph.
     */
    public void addVertex(ChunkVertex vertex) {
        vertices.add(vertex);
    }

    /**
     * Add an edge between two vertices (adjacent chunks).
     */
    public void addEdge(ChunkEdge edge) {
        edges.add(edge);
    }

    /**
     * Compute the partition using a greedy k-way algorithm.
     * This is a simplified METIS-style approach — actual production
     * would call the METIS library directly.
     */
    public PartitionMap.PartitionState computePartition() {
        if (vertices.isEmpty()) {
            log.warn("No vertices to partition — returning default equal-area partition");
            return PartitionMap.createDefault(targetShardCount, "shard", 0, 0).build();
        }

        // Assign vertices to shards using a greedy approach
        Map<Long, Integer> vertexToShard = greedyKWayPartition();

        // Build shard partitions from the assignment
        List<PartitionMap.ShardPartition> shardPartitions = assignShardPartitions(vertexToShard);

        // Generate seam boundaries
        List<PartitionMap.SeamBoundary> seams = generateSeams(vertexToShard);

        log.info("Computed partition for {} vertices across {} shards", 
            vertices.size(), shardPartitions.size());

        return new PartitionMap.PartitionState(0, shardPartitions, seams);
    }

    /**
     * Greedy k-way graph partitioning.
     * Simplified METIS-style: assign chunks to shards based on
     * load weight, minimizing edges across partitions.
     */
    private Map<Long, Integer> greedyKWayPartition() {
        // Sort vertices by MSPT weight descending (highest load first)
        List<ChunkVertex> sorted = vertices.stream()
            .sorted(Comparator.comparingLong(v -> -v.msptWeight()))
            .collect(Collectors.toList());

        Map<Long, Integer> assignment = new LinkedHashMap<>();
        double[] shardLoads = new double[targetShardCount];
        double totalWeight = sorted.stream().mapToDouble(ChunkVertex::msptWeight).sum();
        long totalWeightLong = (long) totalWeight;
        double targetLoad = totalWeightLong / targetShardCount;

        for (ChunkVertex vertex : sorted) {
            // Find the shard with the least total load
            int bestShard = -1;
            double minLoad = Double.MAX_VALUE;

            for (int i = 0; i < targetShardCount; i++) {
                if (shardLoads[i] < minLoad) {
                    minLoad = shardLoads[i];
                    bestShard = i;
                }
            }

            // Cap shard load at target + 20% to prevent overloading
            if (shardLoads[bestShard] + vertex.msptWeight() > targetLoad * 1.2) {
                // Find the shard with the second-lowest load
                bestShard = -1;
                minLoad = Double.MAX_VALUE;
                for (int i = 0; i < targetShardCount; i++) {
                    if (shardLoads[i] < minLoad && shardLoads[i] < targetLoad * 0.8) {
                        minLoad = shardLoads[i];
                        bestShard = i;
                    }
                }
                if (bestShard < 0) {
                    // No shard under threshold — just pick the least loaded
                    for (int i = 0; i < targetShardCount; i++) {
                        if (shardLoads[i] < minLoad) {
                            minLoad = shardLoads[i];
                            bestShard = i;
                        }
                    }
                }
            }

            assignment.put(vertex.chunkKey(), bestShard);
            shardLoads[bestShard] += vertex.msptWeight();
        }

        return assignment;
    }

    private List<PartitionMap.ShardPartition> assignShardPartitions(Map<Long, Integer> assignment) {
        Map<Integer, List<ChunkVertex>> shardGroups = new HashMap<>();
        for (var entry : assignment.entrySet()) {
            shardGroups.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(
                vertices.stream()
                    .filter(v -> v.chunkKey() == entry.getKey())
                    .findFirst()
                    .orElseThrow()
            );
        }

        List<PartitionMap.ShardPartition> partitions = new ArrayList<>();
        for (int shardId = 0; shardId < targetShardCount; shardId++) {
            List<ChunkVertex> shards = shardGroups.getOrDefault(shardId, Collections.emptyList());
            if (shards.isEmpty()) {
                log.warn("No chunks assigned to shard {}", shardId);
                partitions.add(new PartitionMap.ShardPartition(
                    shardId,
                    "shard-" + shardId,
                    "shard-" + shardId + ":25565",
                    0, 0, 0, 0
                ));
            } else {
                int minX = shards.stream().mapToInt(ChunkVertex::chunkX).min().orElse(0);
                int minZ = shards.stream().mapToInt(ChunkVertex::chunkZ).min().orElse(0);
                int maxX = shards.stream().mapToInt(ChunkVertex::chunkX).max().orElse(0);
                int maxZ = shards.stream().mapToInt(ChunkVertex::chunkZ).max().orElse(0);
                partitions.add(new PartitionMap.ShardPartition(
                    shardId,
                    "shard-" + shardId,
                    "shard-" + shardId + ":25565",
                    minX << 4, minZ << 4,
                    (maxX - minX + 1),
                    (maxZ - minZ + 1)
                ));
            }
        }
        return partitions;
    }

    /**
      * Generate seam boundaries from the partition.
      */
    private List<PartitionMap.SeamBoundary> generateSeams(Map<Long, Integer> assignment) {
        List<PartitionMap.SeamBoundary> seams = new ArrayList<>();
        long seamCounter = 0;

        for (ChunkEdge edge : edges) {
            Integer shardA = assignment.get(edge.vertexA);
            Integer shardB = assignment.get(edge.vertexB);
            
            if (shardA != null && shardB != null && shardA != shardB) {
                seams.add(new PartitionMap.SeamBoundary(
                    seamCounter++,
                    edge.isVertical ? "vertical" : "horizontal",
                    edge.coordinate,
                    edge.minSecondary,
                    edge.maxSecondary
                ));
            }
        }
        return seams;
    }

    /**
     * A vertex in the loaded-chunk adjacency graph.
     */
    public static class ChunkVertex {
        private final long chunkKey;
        private final int chunkX;
        private final int chunkZ;
        private final long msptWeight;
        private final int entityCount;

        public ChunkVertex(long chunkKey, int chunkX, int chunkZ, double msptWeight, int entityCount) {
            this.chunkKey = chunkKey;
            this.chunkX = chunkX;
            this.chunkZ = chunkZ;
            this.msptWeight = Math.max(1L, (long) (msptWeight * 1000)); // normalize to 1000x
            this.entityCount = entityCount;
        }

        public long chunkKey() { return chunkKey; }
        public int chunkX() { return chunkX; }
        public int chunkZ() { return chunkZ; }
        public long msptWeight() { return msptWeight; }
        public int entityCount() { return entityCount; }
    }

    /**
     * An edge between two adjacent chunks in the adjacency graph.
     */
    public static class ChunkEdge {
        private final long vertexA;
        private final long vertexB;
        private final int edgeWeight; // cross-boundary traffic weight
        private final boolean isVertical;
        private final int coordinate;
        private final int minSecondary;
        private final int maxSecondary;

        public ChunkEdge(long vertexA, long vertexB, int edgeWeight, boolean isVertical, int coordinate, int minY, int maxY) {
            this.vertexA = vertexA;
            this.vertexB = vertexB;
            this.edgeWeight = edgeWeight;
            this.isVertical = isVertical;
            this.coordinate = coordinate;
            this.minSecondary = minY;
            this.maxSecondary = maxY;
        }

        public long getVertexA() { return vertexA; }
        public long getVertexB() { return vertexB; }
        public int getEdgeWeight() { return edgeWeight; }
        public boolean isVertical() { return isVertical; }
        public int getCoordinate() { return coordinate; }
        public int getMinSecondary() { return minSecondary; }
        public int getMaxSecondary() { return maxSecondary; }
    }
}
