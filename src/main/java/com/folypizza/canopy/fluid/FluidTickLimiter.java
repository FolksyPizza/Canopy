package com.folypizza.canopy.fluid;

import com.folypizza.canopy.leader.PartitionMap;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-region fluid tick budget limiter.
 *
 * Caps fluid ticks per region per tick. Excess fluid ticks are queued
 * for the next tick phase. This bounds worst-case MSPT during floods
 * and turns a server-killing simultaneous flood into slow-spreading water.
 *
 * This is orthogonally independent of distribution and can be deployed
 * standalone.
 */
public class FluidTickLimiter {
    private static final Logger log = LoggerFactory.getLogger(FluidTickLimiter.class);

    private final long regionId;
    private final long maxFluIDTICKS_PER_TICK;
    private final long maxDeferred;
    private final AtomicLong fluidTicksSinceReset = new AtomicLong(0);
    private final AtomicLong deferredCount = new AtomicLong(0);
    private final ConcurrentLinkedQueue<FluidTick> deferredQueue = new ConcurrentLinkedQueue<>();
    private final int activeRegionCount;

    public FluidTickLimiter(long regionId, long maxFluidTicksPerTick, long maxDeferred) {
        this.regionId = regionId;
        this.maxFluIDTICKS_PER_TICK = maxFluidTicksPerTick;
        this.maxDeferred = maxDeferred;
        this.activeRegionCount = 0;
        log.info("Initialized fluid tick limiter for region {} (budget: {}/tick, defer limit: {})",
            regionId, maxFluidTicksPerTick, maxDeferred);
    }
    public FluidTickLimiter(long regionId, long maxFluidTicksPerTick) {
        this(regionId, maxFluidTicksPerTick, 2000);
    }

    /**
     * Attempt to process a fluid tick. Returns false if budget is exceeded.
     * True means the tick was processed (either directly or added to the deferred queue).
     */
    public boolean tryScheduleFluidTick(FluidTick tick) {
        long current = fluidTicksSinceReset.get();
        if (current < maxFluIDTICKS_PER_TICK) {
            // Budget not exhausted — process directly
            if (fluidTicksSinceReset.compareAndSet(current, current + 1)) {
                tick.setProcessed();
                return true;
            }
            // CAS lost — another tick won the slot. Try again via deferred.
        }

        // Budget exhausted — defer to next tick phase
        if (deferredCount.get() < maxDeferred) {
            deferredQueue.add(tick);
            deferredCount.incrementAndGet();
            log.debug("Defered fluid tick for region {} (queue: {}/{})", 
                regionId, deferredCount.get(), maxDeferred);
            return true;  // tick was queued, will be processed next phase
        }

        // Queue full — drop the tick (it will re-queue when the block is re-ticked)
        log.debug("Dropping fluid tick for region {} (queue full: {}/{})",
            regionId, deferredCount.get(), maxDeferred);
        tick.dropped();
        return false;
    }

    /**
     * Process deferred ticks from the previous phase.
     * Returns true if any were processed.
     */
    public boolean processDeferredTicks() {
        boolean anyProcessed = false;
        while (deferredCount.get() > 0 && 
               fluidTicksSinceReset.get() < maxFluIDTICKS_PER_TICK) {
            FluidTick tick = deferredQueue.poll();
            if (tick != null) {
                tick.setProcessed();
                fluidTicksSinceReset.incrementAndGet();
                deferredCount.decrementAndGet();
                anyProcessed = true;
            }
        }
        return anyProcessed;
    }

    /**
     * Reset the tick counter at the start of each tick phase.
     * Called once per region per tick by the scheduler.
     */
    public void resetTickCounter() {
        fluidTicksSinceReset.set(0);
    }

    /**
     * Get current utilization (0.0-1.0).
     */
    public double getFluidTickUtilization() {
        return fluidTicksSinceReset.get() / (double) maxFluIDTICKS_PER_TICK;
    }

    /**
     * Check if this region is being constrained on fluid ticks.
     */
    public boolean isFluidLimited() {
        return fluidTicksSinceReset.get() >= maxFluIDTICKS_PER_TICK;
    }

    /**
     * Get the deferred queue size.
     */
    public long getDeferredCount() {
        return deferredCount.get();
    }

    /**
     * Get the total number of ticks processed (including deferred).
     */
    public long getTotalTicksProcessed() {
        return fluidTicksSinceReset.get() + (maxFluIDTICKS_PER_TICK - fluidTicksSinceReset.get());
    }

    /**
     * A fluid tick that can be deferred.
     */
    public static class FluidTick {
        private final long regionId;
        private final String type; // WATER or LAVA
        private final int x, y, z;
        private boolean processed;
        private boolean dropped;

        public FluidTick(long regionId, String type, int x, int y, int z) {
            this.regionId = regionId;
            this.type = type;
            this.x = x;
            this.y = y;
            this.z = z;
            this.processed = false;
            this.dropped = false;
        }

        public void setProcessed() { this.processed = true; }
        public boolean isProcessed() { return processed; }
        public void dropped() { this.dropped = true; processed = false; }
        public boolean isDropped() { return dropped; }

        @Override
        public String toString() {
            return String.format("FluidTick(%s, %d,%d,%d)", type, x, y, z);
        }
    }
}
