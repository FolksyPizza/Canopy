package com.folypizza.canopy.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class MigrationScheduler {
    private static final Logger log = LoggerFactory.getLogger(MigrationScheduler.class);
    private static final int MIGRATION_WORKER_POOL = 4;

    private final java.util.concurrent.ExecutorService migrationWorkers =
        java.util.concurrent.Executors.newFixedThreadPool(MIGRATION_WORKER_POOL, r -> {
            Thread t = new Thread(r, "canopy-migration-worker");
            t.setDaemon(true);
            return t;
        });

    private final ScheduledExecutorService periodicScheduler =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "canopy-migration-scheduler");
            t.setDaemon(true);
            return t;
        });

    public void scheduleMigrationWork(Runnable work) {
        migrationWorkers.submit(work);
    }

    public void schedulePeriodicWork(Runnable work, long intervalMs) {
        periodicScheduler.scheduleAtFixedRate(work, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> runInMigrationWorker(Runnable work) {
        return CompletableFuture.runAsync(() -> {
            try {
                work.run();
            } catch (Exception e) {
                log.error("Migration worker task failed", e);
            }
        }, migrationWorkers);
    }

    public void shutdown() {
        migrationWorkers.shutdown();
        periodicScheduler.shutdown();
    }
}
