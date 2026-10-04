package com.folksypizza.canopy.session;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Authenticated private-network control endpoint with bounded request workers and payloads. */
public final class SessionControlServer implements AutoCloseable {
    public interface Handler { PlayerSession execute(SessionCommand command) throws Exception; }
    private final HttpServer server;
    private final ThreadPoolExecutor workers;
    private final byte[] credential;
    private final Handler handler;
    private final SessionAudit audit;

    public SessionControlServer(InetSocketAddress address, String secret, Handler handler) throws IOException {
        this(address, secret, handler, SessionAudit.NONE);
    }
    public SessionControlServer(InetSocketAddress address, String secret, Handler handler, SessionAudit audit) throws IOException {
        if (secret == null || secret.length() < 16) throw new IllegalArgumentException("Session control secret is required");
        credential = ("Bearer " + secret).getBytes(StandardCharsets.UTF_8);
        this.handler = handler; this.audit = audit;
        server = HttpServer.create(address, 128);
        workers = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(128), task -> {
            Thread t = new Thread(task, "canopy-session-control"); t.setDaemon(true); return t;
        });
        workers.setRejectedExecutionHandler((work, pool) -> {
            audit.event("control.queue_rejected", java.util.Map.of("queued", pool.getQueue().size()));
            throw new java.util.concurrent.RejectedExecutionException("Session control queue full or stopped");
        });
        server.setExecutor(workers);
        server.createContext("/canopy/session", this::handle);
    }

    public void start() { server.start(); }
    public int port() { return server.getAddress().getPort(); }

    private void handle(HttpExchange exchange) throws IOException {
        var fields = new java.util.LinkedHashMap<String, Object>();
        String request = exchange.getRequestHeaders().getFirst("X-Canopy-Audit-Request");
        try { fields.put("request", java.util.UUID.fromString(request)); }
        catch (RuntimeException invalid) { fields.put("request", java.util.UUID.randomUUID()); }
        long started = System.nanoTime();
        int status = 503;
        try (exchange) {
            String presented = exchange.getRequestHeaders().getFirst("Authorization");
            byte[] provided = presented == null ? new byte[0] : presented.getBytes(StandardCharsets.UTF_8);
            if (!MessageDigest.isEqual(credential, provided)) { status = 401; reply(exchange, status, null); return; }
            if (!exchange.getRequestMethod().equals("POST")) { status = 405; reply(exchange, status, null); return; }
            byte[] requestBody = exchange.getRequestBody().readNBytes(8193);
            if (requestBody.length > 8192) { status = 413; reply(exchange, status, null); return; }
            try {
                SessionCommand command = SessionCommand.decode(requestBody);
                fields.putAll(SessionAudit.fields(command)); audit.event("control.accepted", fields);
                PlayerSession state = handler.execute(command);
                fields.putAll(SessionAudit.fields(state));
                status = state == null ? 204 : 200;
                reply(exchange, status, state == null ? null : SessionCodec.encode(state));
            } catch (SessionCoordinator.Conflict conflict) {
                status = 409; audit.failure("control.conflict", fields, conflict); reply(exchange, status, null);
            } catch (IllegalArgumentException | NullPointerException | IOException malformed) {
                status = 400; audit.failure("control.invalid", fields, malformed); reply(exchange, status, null);
            } catch (Exception unavailable) {
                status = 503; audit.failure("control.unavailable", fields, unavailable); reply(exchange, status, null);
            }
        } catch (IOException failed) {
            audit.failure("control.transport_failed", fields, failed); throw failed;
        } finally {
            fields.put("status", status); fields.put("durationMs", (System.nanoTime() - started) / 1_000_000);
            audit.event("control.completed", fields);
        }
    }

    private static void reply(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body == null ? -1 : body.length);
        if (body != null) exchange.getResponseBody().write(body);
    }

    @Override public void close() { server.stop(0); workers.shutdownNow(); }
}
