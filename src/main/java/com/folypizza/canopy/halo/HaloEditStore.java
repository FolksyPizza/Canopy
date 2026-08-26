package com.folypizza.canopy.halo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records block edits near the shard boundary so the peer shard can mirror them into its
 * halo strip (making cross-seam edits visible on the other side).
 *
 * Each edit gets a monotonically increasing sequence number; peers pull edits with
 * {@code seq > lastSeen} so only new changes cross the wire. The deque is capped.
 */
public class HaloEditStore {
    private static final int MAX_EDITS = 8192;

    public record Edit(long seq, int x, int y, int z, String data) {}

    private final AtomicLong seq = new AtomicLong(0);
    private final ConcurrentLinkedDeque<Edit> edits = new ConcurrentLinkedDeque<>();

    public void record(int x, int y, int z, String data) {
        edits.addLast(new Edit(seq.incrementAndGet(), x, y, z, data));
        while (edits.size() > MAX_EDITS) edits.pollFirst();
    }

    public long currentSeq() {
        return seq.get();
    }

    /** Edits with seq &gt; sinceSeq whose x is within [minX, maxX]. */
    public List<Edit> since(long sinceSeq, int minX, int maxX) {
        List<Edit> out = new ArrayList<>();
        for (Edit e : edits) {
            if (e.seq() > sinceSeq && e.x() >= minX && e.x() <= maxX) {
                out.add(e);
            }
        }
        return out;
    }
}
