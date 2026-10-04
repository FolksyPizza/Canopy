package com.folksypizza.canopy.session;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Blocking bounded-time control calls; invoke on I/O workers, never a player or connection thread. */
public final class SessionControlClient {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String secret;
    private final SessionAudit audit;

    public SessionControlClient(String secret) { this(secret, SessionAudit.NONE); }
    public SessionControlClient(String secret, SessionAudit audit) {
        if (secret == null || secret.length() < 16) throw new IllegalArgumentException("Session control secret is required");
        this.secret = secret; this.audit = audit;
    }

    public PlayerSession request(URI endpoint, SessionCommand command) throws IOException, InterruptedException, SQLException {
        var fields = SessionAudit.fields(command);
        String requestId = java.util.UUID.randomUUID().toString();
        fields.put("request", requestId);
        long start = System.nanoTime();
        audit.event("control.send", fields);
        try {
            PlayerSession state = perform(endpoint, command, requestId);
            fields.putAll(SessionAudit.fields(state)); fields.put("durationMs", (System.nanoTime() - start) / 1_000_000);
            audit.event("control.received", fields); return state;
        } catch (IOException | InterruptedException | SQLException | RuntimeException failed) {
            fields.put("durationMs", (System.nanoTime() - start) / 1_000_000);
            audit.failure("control.failed", fields, failed); throw failed;
        }
    }

    private PlayerSession perform(URI endpoint, SessionCommand command, String requestId)
            throws IOException, InterruptedException, SQLException {
        if (!"http".equals(endpoint.getScheme()) && !"https".equals(endpoint.getScheme())) {
            throw new IllegalArgumentException("Session endpoint must use HTTP or HTTPS");
        }
        HttpRequest request = HttpRequest.newBuilder(endpoint.resolve("/canopy/session"))
            .timeout(Duration.ofSeconds(4)).header("X-Canopy-Audit-Request", requestId).header("Authorization", "Bearer " + secret)
            .POST(HttpRequest.BodyPublishers.ofByteArray(command.encode())).build();
        var pending = client.sendAsync(request, ignored -> new BoundedBody());
        HttpResponse<byte[]> response;
        try {
            response = pending.get(4, TimeUnit.SECONDS);
        } catch (TimeoutException timeout) {
            pending.cancel(true);
            throw new IOException("Session control timed out", timeout);
        } catch (InterruptedException interrupted) {
            pending.cancel(true);
            throw interrupted;
        } catch (ExecutionException failed) {
            throw new IOException("Session control failed", failed.getCause());
        }
        var responseFields = SessionAudit.fields(command);
        responseFields.put("request", requestId); responseFields.put("status", response.statusCode());
        audit.event("control.response", responseFields);
        if (response.statusCode() == 204) return null;
        if (response.statusCode() == 409) throw new SessionCoordinator.Conflict("Control operation conflicts with current session");
        if (response.statusCode() != 200) throw new IOException("Session control returned HTTP " + response.statusCode());
        return SessionCodec.decode(response.body());
    }

    /** Limit memory while receiving, rather than validating after an unbounded allocation. */
    private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private Flow.Subscription subscription;
        @Override public CompletionStage<byte[]> getBody() { return result; }
        @Override public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }
        @Override public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > PlayerSession.MAX_SNAPSHOT_BYTES + 2048 - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(new IOException("Session control payload too large"));
                    return;
                }
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                bytes.writeBytes(chunk);
            }
            subscription.request(1);
        }
        @Override public void onError(Throwable error) { result.completeExceptionally(error); }
        @Override public void onComplete() { result.complete(bytes.toByteArray()); }
    }
}
