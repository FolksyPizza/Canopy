package com.folksypizza.canopy.velocity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GatewaySessionsEventLoopTest {
    @Test
    void workRunsOnTheConnectionLoop() throws Exception {
        ExecutorService loop = Executors.newSingleThreadExecutor(r -> new Thread(r, "synthetic-connection-loop"));
        try {
            var ranOn = new java.util.concurrent.atomic.AtomicReference<String>();
            GatewaySessions.dispatchOnEventLoop(loop, () -> ranOn.set(Thread.currentThread().getName()))
                .get(2, TimeUnit.SECONDS);
            assertEquals("synthetic-connection-loop", ranOn.get());
        } finally {
            loop.shutdownNow();
        }
    }

    @Test
    void actionFailureIsReportedToTheCaller() {
        var failure = new IllegalStateException("synthetic failure");
        var result = GatewaySessions.dispatchOnEventLoop(Runnable::run, () -> { throw failure; });
        ExecutionException observed = assertThrows(ExecutionException.class,
            () -> result.get(2, TimeUnit.SECONDS));
        assertTrue(observed.getCause() == failure);
    }

    @Test
    void rejectedLoopDoesNotRunWork() {
        ExecutorService loop = Executors.newSingleThreadExecutor();
        loop.shutdownNow();
        var ran = new java.util.concurrent.atomic.AtomicBoolean();
        var result = GatewaySessions.dispatchOnEventLoop(loop, () -> ran.set(true));
        assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        assertTrue(!ran.get());
    }
}
