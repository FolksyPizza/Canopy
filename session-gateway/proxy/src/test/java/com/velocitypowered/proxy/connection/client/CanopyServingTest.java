package com.velocitypowered.proxy.connection.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CanopyServingTest {
  private CanopyServing.Work work(Runnable compute, CompletableFuture<Void> delivered, Runnable discard) {
    return new CanopyServing.Work() {
      public java.util.concurrent.CompletionStage<Void> run() { compute.run(); return delivered; }
      public void discard() { discard.run(); }
    };
  }

  @Test void nextPacketWaitsForPreviousDeliveryNotJustComputation() throws Exception {
    try (CanopyServing pool = new CanopyServing(2, 2)) {
      CanopyServing.Lane lane = pool.newLane();
      CountDownLatch computed = new CountDownLatch(1);
      CountDownLatch second = new CountDownLatch(1);
      CompletableFuture<Void> delivery = new CompletableFuture<>();
      lane.submit(work(computed::countDown, delivery, () -> fail("Unexpected discard")));
      lane.submit(work(second::countDown, CompletableFuture.completedFuture(null), () -> fail("Unexpected discard")));
      assertTrue(computed.await(2, TimeUnit.SECONDS));
      assertFalse(second.await(50, TimeUnit.MILLISECONDS));
      delivery.complete(null);
      assertTrue(second.await(2, TimeUnit.SECONDS));
    }
  }

  @Test void differentPlayersComputeConcurrently() throws Exception {
    try (CanopyServing pool = new CanopyServing(2, 2)) {
      CountDownLatch both = new CountDownLatch(2);
      CountDownLatch release = new CountDownLatch(1);
      Runnable compute = () -> {
        both.countDown();
        try { assertTrue(release.await(2, TimeUnit.SECONDS)); }
        catch (InterruptedException e) { throw new AssertionError(e); }
      };
      pool.newLane().submit(work(compute, CompletableFuture.completedFuture(null), () -> {}));
      pool.newLane().submit(work(compute, CompletableFuture.completedFuture(null), () -> {}));
      assertTrue(both.await(2, TimeUnit.SECONDS));
      release.countDown();
    }
  }

  @Test void sourceAndShadowShareOneOrderedLane() throws Exception {
    try (CanopyServing pool = new CanopyServing(3, 2)) {
      List<Integer> order = new ArrayList<>();
      CountDownLatch done = new CountDownLatch(100);
      CanopyServing.Lane lane = pool.newLane();
      for (int i = 0; i < 100; i++) {
        int index = i;
        lane.submit(work(() -> { order.add(index); done.countDown(); },
            CompletableFuture.completedFuture(null), () -> fail("Unexpected discard")));
      }
      assertTrue(done.await(2, TimeUnit.SECONDS));
      assertEquals(java.util.stream.IntStream.range(0, 100).boxed().toList(), order);
    }
  }

  @Test void saturatedPoolRetriesWithoutRunningOnCaller() throws Exception {
    try (CanopyServing pool = new CanopyServing(1, 1)) {
      CountDownLatch blocked = new CountDownLatch(1), release = new CountDownLatch(1), done = new CountDownLatch(3);
      String caller = Thread.currentThread().getName();
      pool.newLane().submit(work(() -> {
        blocked.countDown();
        try { release.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
      }, CompletableFuture.completedFuture(null), () -> {}));
      assertTrue(blocked.await(2, TimeUnit.SECONDS));
      for (int i = 0; i < 3; i++) pool.newLane().submit(work(() -> {
        assertNotEquals(caller, Thread.currentThread().getName());
        done.countDown();
      }, CompletableFuture.completedFuture(null), () -> {}));
      release.countDown();
      assertTrue(done.await(2, TimeUnit.SECONDS));
    }
  }

  @Test void closingLaneDiscardsEachQueuedPacketOnce() throws Exception {
    try (CanopyServing pool = new CanopyServing(1, 1)) {
      CanopyServing.Lane lane = pool.newLane();
      CompletableFuture<Void> firstDelivery = new CompletableFuture<>();
      CountDownLatch started = new CountDownLatch(1);
      AtomicInteger discarded = new AtomicInteger();
      lane.submit(work(started::countDown, firstDelivery, discarded::incrementAndGet));
      assertTrue(started.await(2, TimeUnit.SECONDS));
      lane.submit(work(() -> fail("Closed lane ran queued work"), CompletableFuture.completedFuture(null), discarded::incrementAndGet));
      lane.close();
      lane.close();
      lane.submit(work(() -> fail("Closed lane accepted work"), CompletableFuture.completedFuture(null), discarded::incrementAndGet));
      assertEquals(2, discarded.get());
      firstDelivery.complete(null);
    }
  }

  @Test void failedDeliveryDoesNotStrandTheLane() throws Exception {
    try (CanopyServing pool = new CanopyServing(1, 1)) {
      CanopyServing.Lane lane = pool.newLane();
      CountDownLatch next = new CountDownLatch(1);
      lane.submit(work(() -> {}, CompletableFuture.failedFuture(new IllegalStateException("test failure")), () -> {}));
      lane.submit(work(next::countDown, CompletableFuture.completedFuture(null), () -> {}));
      assertTrue(next.await(2, TimeUnit.SECONDS));
    }
  }
}
