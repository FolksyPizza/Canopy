package com.velocitypowered.proxy.connection.backend;

import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.MinecraftSessionHandler;
import com.velocitypowered.proxy.connection.client.CanopyServing;
import com.velocitypowered.proxy.connection.client.ClientPlaySessionHandler;
import com.velocitypowered.proxy.protocol.MinecraftPacket;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Keeps network ownership on its event loop while chunk computation runs on a player serving lane. */
public final class CanopyServingSessionHandler implements MinecraftSessionHandler {
  private final MinecraftSessionHandler delegate;
  private final MinecraftConnection connection;
  private final ClientPlaySessionHandler player;
  private volatile boolean active = true;
  private volatile int pending;

  private CanopyServingSessionHandler(MinecraftSessionHandler delegate, MinecraftConnection connection,
      ClientPlaySessionHandler player) {
    this.delegate = delegate;
    this.connection = connection;
    this.player = player;
  }

  public static MinecraftSessionHandler wrap(MinecraftSessionHandler handler,
      MinecraftConnection connection, ClientPlaySessionHandler player) {
    if (!Boolean.parseBoolean(System.getProperty("canopy.serving.enabled", "true"))
        || !player.getCanopyRewriter().active(connection.getProtocolVersion())) return handler;
    return new CanopyServingSessionHandler(handler, connection, player);
  }

  @Override public void handleGeneric(MinecraftPacket packet) { enqueue(ReferenceCountUtil.retain(packet)); }
  @Override public void handleUnknown(ByteBuf packet) { enqueue(packet.retain()); }

  private void enqueue(Object packet) {
    // Pause before accepting another receive cycle. The current decoder batch may still drain into this lane.
    connection.setAutoReading(false);
    pending++;
    player.getCanopyServing().submit(new CanopyServing.Work() {
      private List<ByteBuf> replacement;
      private boolean prepared;

      @Override public java.util.concurrent.CompletionStage<Void> run() {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (active && packet instanceof ByteBuf raw) {
          if (delegate instanceof BackendPlaySessionHandler && player.getCanopyHandover()
              .consumeRepairReset(raw, connection, player.getCanopyRewriter())) {
            replacement = List.of();
          } else {
            replacement = delegate instanceof CanopyShadowSessionHandler
              ? player.getCanopyChunkView().onShadow(raw, connection.getChannel().alloc())
              : player.getCanopyChunkView().onClientbound(raw, connection.getChannel().alloc());
          }
          prepared = true;
        }
        try {
          connection.eventLoop().execute(() -> {
            try {
              if (active && !connection.isClosed() && !delegate.beforeHandle()) {
                if (packet instanceof MinecraftPacket decoded) {
                  if (!decoded.handle(delegate)) delegate.handleGeneric(decoded);
                } else if (delegate instanceof BackendPlaySessionHandler backend) {
                  List<ByteBuf> transferred = replacement;
                  replacement = null;
                  backend.handlePreparedUnknown((ByteBuf) packet, transferred); // downstream owns all replacements after delivery
                } else if (delegate instanceof CanopyShadowSessionHandler shadow) {
                  List<ByteBuf> transferred = replacement;
                  replacement = null;
                  shadow.handlePreparedUnknown(transferred);
                }
              } else if (prepared) {
                player.getCanopyChunkView().invalidateAll();
              }
            } catch (Throwable failure) {
              com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks.audit(player.getCanopyPlayerId(),
                  "serving.failed", "queued", pending, "error", failure.getClass().getName());
              delegate.exception(failure);
            } finally {
              release();
              completed();
              done.complete(null);
            }
          });
        } catch (java.util.concurrent.RejectedExecutionException closedLoop) {
          com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks.audit(player.getCanopyPlayerId(), "serving.loop_closed", "queued", pending);
          release();
          done.complete(null);
        }
        return done;
      }

      @Override public void discard() {
        com.velocitypowered.proxy.connection.client.CanopyLifecycleHooks.audit(player.getCanopyPlayerId(), "serving.discarded", "queued", pending);
        release();
        try { connection.eventLoop().execute(() -> completed()); }
        catch (java.util.concurrent.RejectedExecutionException closedLoop) { /* connection is gone */ }
      }

      private void release() {
        ReferenceCountUtil.release(packet);
        if (replacement != null) { replacement.forEach(ReferenceCountUtil::release); replacement = null; }
      }
    });
  }

  private void completed() {
    if (--pending == 0 && active && !connection.isClosed()) {
      delegate.readCompleted();
      if (player.getCanopyClientConnection().getChannel().isWritable()) connection.setAutoReading(true);
    }
  }

  public boolean canRead() { return pending == 0 && active; }

  @Override public void readCompleted() { if (pending == 0) delegate.readCompleted(); }
  @Override public void activated() { delegate.activated(); }
  @Override public void deactivated() { active = false; delegate.deactivated(); }
  @Override public void disconnected() { active = false; delegate.disconnected(); }
  @Override public void connected() { delegate.connected(); }
  @Override public void exception(Throwable error) { delegate.exception(error); }
  @Override public void writabilityChanged() { delegate.writabilityChanged(); }
}
