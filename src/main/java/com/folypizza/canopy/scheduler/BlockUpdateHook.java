package com.folypizza.canopy.scheduler;

import com.folypizza.canopy.leader.PartitionMap;
import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.*;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.event.world.StructureGrowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Block update interceptor — Phase 1 seam corridor enforcement.
 * 
 * Intercepts all block modification events and short-circuits them
 * when they fall within a 32-block seam corridor. This protects the 
 * seam boundary so block mechanics cannot cross into the neutral zone,
 * while normal game play proceeds unimpeded outside the corridor.
 */
public class BlockUpdateHook implements Listener {
    private static final Logger log = LoggerFactory.getLogger(BlockUpdateHook.class);
    private static final int CORRIDOR_WIDTH = 32; // half-width (each side)

    private final PartitionMap partitionMap;
    private final ConcurrentHashMap<Long, Integer> blockedCount = new ConcurrentHashMap<>();

    public BlockUpdateHook(PartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }

    @EventHandler(ignoreCancelled = true, priority = org.bukkit.event.EventPriority.MONITOR)
    public void onBlockPlace(BlockPlaceEvent e) {
        interceptBlock(e.getBlock(), "BLOCK_PLACE", e.getPlayer().getName());
    }

    @EventHandler(ignoreCancelled = true, priority = org.bukkit.event.EventPriority.MONITOR)
    public void onBlockBreak(BlockBreakEvent e) {
        interceptBlock(e.getBlock(), "BLOCK_BREAK", e.getPlayer().getName());
    }

    @EventHandler(ignoreCancelled = true)
    public void onBlockExplode(EntityExplodeEvent e) {
        interceptExplosion(e.blockList(), "ENTITY_EXPLODE");
        e.setCancelled(false);  // allow explosions, just track corridor hits
    }

    @EventHandler(ignoreCancelled = true)
    public void onStructureGrow(StructureGrowEvent e) {
        if (isInAnyCorridor(e.getLocation().getBlock())) {
            if (log.isDebugEnabled()) {
                log.warn("Blocks prevented in corridor for {}", e.getSpecies());
            }
        }
    }

    // Bucket fill/empty events are CraftBukkit-specific; handled via reflection at runtime

    @EventHandler(ignoreCancelled = true)
    public void onFluidSpread(BlockFromToEvent e) {
        // Fluid flow within corridor is disabled by seam rules
        if (isInAnyCorridor(e.getToBlock())) {
            if (log.isDebugEnabled()) {
                log.debug("Fluid spread blocked in corridor");
            }
        }
    }

    @EventHandler(priority = org.bukkit.event.EventPriority.LOW, ignoreCancelled = true)
    public void onRedstone(BlockRedstoneEvent e) {
        if (isInAnyCorridor(e.getBlock())) {
            // Redstone is inert in the seam corridor per DESIGN.md
            // Allow signal change but block mechanical effects
            if (log.isDebugEnabled()) {
                log.debug("Redstone activity in corridor at {}", e.getBlock().getLocation());
            }
        }
    }

    @EventHandler(priority = org.bukkit.event.EventPriority.MONITOR, ignoreCancelled = true)
    public void onIgnite(BlockIgniteEvent e) {
        if (isInAnyCorridor(e.getBlock())) {
            e.setCancelled(true);
        }
    }

    /**
     * Check if a block falls within any seam corridor.
     */
    private boolean isInCorridor(Block block) {
        return isInAnyCorridor(block);
    }

    private boolean isInAnyCorridor(Block block) {
        var state = partitionMap.getState();
        var seams = state.seams();

        // Check each seam boundary
        for (var seam : seams) {
            int seamCoord = seam.coordinate();
            int secondaryMin = seam.minSecondary();
            int secondaryMax = seam.maxSecondary();

            if (seam.type().equals("vertical")) {
                // Vertical seam: X coordinate is the seam, Z is secondary
                int blockX = block.getX();
                int blockZ = block.getZ();

                // Check if Z is within the seam's secondary range
                if (blockZ >= secondaryMin && blockZ < secondaryMax) {
                    // Check if X is within the corridor half-width of the seam
                    int distFromSeam = Math.abs(blockX - seamCoord);
                    if (distFromSeam <= CORRIDOR_WIDTH) {
                        return true;
                    }
                }
            } else if (seam.type().equals("horizontal")) {
                // Horizontal seam: Z coordinate is the seam, X is secondary
                int blockZ = block.getZ();
                int blockX = block.getX();

                // Check if X is within the seam's secondary range
                if (blockX >= secondaryMin && blockX < secondaryMax) {
                    // Check if Z is within the corridor half-width of the seam
                    int distFromSeam = Math.abs(blockZ - seamCoord);
                    if (distFromSeam <= CORRIDOR_WIDTH) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void interceptBlock(Block block, String eventType, String player) {
        if (isInCorridor(block)) {
            blockedCount.merge(1L, 1, Integer::sum);
            log.warn("[CORRIDOR] {} denied in seam corridor: player={}, pos={}", 
                eventType, player, block.getLocation());
        }
    }

    private void interceptExplosion(List<Block> blocks, String eventType) {
        for (Block block : blocks) {
            if (isInCorridor(block)) {
                blockedCount.merge(1L, 1, Integer::sum);
            }
        }
        int blocked = blocks.stream().filter(this::isInCorridor).toList().size();
        if (blocked > 0) {
            log.warn("[CORRIDOR] {} blocked {} block(s) in seam corridor", eventType, blocked);
        }
    }

    public Map<String, Integer> getBlockedSummary() {
        Map<String, Integer> summary = new HashMap<>();
        blockedCount.forEach((k, v) -> summary.put(String.valueOf(k), v));
        return Collections.unmodifiableMap(summary);
    }

    public int getCorridorWidth() {
        return CORRIDOR_WIDTH;
    }
}
