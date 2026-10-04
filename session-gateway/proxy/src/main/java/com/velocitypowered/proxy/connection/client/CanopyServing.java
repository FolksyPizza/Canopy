package com.velocitypowered.proxy.connection.client;

import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Parallel computation across players, ordered computation and delivery within each player. */
public final class CanopyServing implements AutoCloseable {
  public interface Work {
    CompletionStage<Void> run();
    void discard();
  }

  private final ThreadPoolExecutor workers;
  private final ScheduledExecutorService retries;

  public CanopyServing(int threads, int readyCapacity) {
    if (threads < 1 || threads > 128 || readyCapacity < 1) {
      throw new IllegalArgumentException("Invalid Canopy serving limits");
    }
    AtomicInteger number = new AtomicInteger();
    workers = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(readyCapacity), task -> {
          Thread thread = new Thread(task, "canopy-serving-" + number.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        });
    retries = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "canopy-serving-admission");
      thread.setDaemon(true);
      return thread;
    });
  }

  private static final class Shared {
    static final CanopyServing INSTANCE = new CanopyServing(
        Integer.getInteger("canopy.serving.threads", Math.max(2,
            Math.min(8, Runtime.getRuntime().availableProcessors() / 2))),
        Integer.getInteger("canopy.serving.readyCapacity", 512));
  }

  public static Lane playerLane() { return Shared.INSTANCE.new Lane(); }

  public Lane newLane() { return new Lane(); }

  public final class Lane implements AutoCloseable {
    private final ArrayDeque<Work> pending = new ArrayDeque<>();
    private boolean running;
    private boolean closed;

    public void submit(Work work) {
      boolean start = false;
      boolean rejected;
      synchronized (this) {
        rejected = closed;
        if (!rejected) {
          pending.add(work);
          if (!running) { running = true; start = true; }
        }
      }
      if (rejected) { work.discard(); return; }
      if (start) schedule();
    }

    private void schedule() {
      try {
        workers.execute(this::runOne);
      } catch (java.util.concurrent.RejectedExecutionException rejected) {
        if (workers.isShutdown()) { close(); return; }
        // Never block an event loop or compute inline on admission pressure. Backend reads remain paused.
        try { retries.schedule(this::schedule, 5, TimeUnit.MILLISECONDS); }
        catch (java.util.concurrent.RejectedExecutionException stopped) { close(); }
      }
    }

    private void runOne() {
      Work work;
      synchronized (this) {
        work = pending.poll();
        if (work == null) { running = false; return; }
      }
      CompletionStage<Void> delivered;
      try { delivered = work.run(); }
      catch (Throwable failed) {
        work.discard();
        delivered = CompletableFuture.failedFuture(failed);
      }
      delivered.whenComplete((ignored, failure) -> {
        boolean next;
        synchronized (this) {
          next = !closed && !pending.isEmpty();
          if (!next) running = false;
        }
        if (next) schedule();
      });
    }

    @Override public void close() {
      ArrayDeque<Work> abandoned;
      synchronized (this) {
        if (closed) return;
        closed = true;
        abandoned = new ArrayDeque<>(pending);
        pending.clear();
      }
      abandoned.forEach(Work::discard);
    }
  }

  @Override public void close() {
    workers.shutdown();
    retries.shutdown();
  }
}
