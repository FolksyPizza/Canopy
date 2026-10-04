package com.folksypizza.canopy.session;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SessionControlTest {
    private static final String CREDENTIAL = "synthetic-control-test-credential";
    private final SessionCommand command = new SessionCommand(SessionCommand.Operation.OPEN,
        UUID.randomUUID(), UUID.randomUUID(), "test-gateway", null, null, null, 0);

    @Test void authenticatedTransportRoundTripsStateAndMissingState() throws Exception {
        var state = new PlayerSession(command.playerId(), command.sessionId(), "test-gateway",
            PlayerSession.Phase.CONNECTED, null, null, null, 1, 1, 0, new byte[]{4, 5});
        try (var server = new SessionControlServer(new InetSocketAddress("127.0.0.1", 0), CREDENTIAL,
            received -> received.operation() == SessionCommand.Operation.FIND ? null : state)) {
            server.start();
            URI endpoint = URI.create("http://127.0.0.1:" + server.port());
            var client = new SessionControlClient(CREDENTIAL);
            PlayerSession found = client.request(endpoint, command);
            assertEquals(state.gatewayToken(), found.gatewayToken());
            assertArrayEquals(state.snapshot(), found.snapshot());
            assertNull(client.request(endpoint, SessionCommand.gateway(SessionCommand.Operation.FIND, state.gatewayToken())));
        }
    }

    @Test void badCredentialsCannotExecuteCommands() throws Exception {
        AtomicInteger executed = new AtomicInteger();
        try (var server = new SessionControlServer(new InetSocketAddress("127.0.0.1", 0), CREDENTIAL,
            received -> { executed.incrementAndGet(); return null; })) {
            server.start();
            assertThrows(IOException.class, () -> new SessionControlClient("wrong-synthetic-credential")
                .request(URI.create("http://127.0.0.1:" + server.port()), command));
            assertEquals(0, executed.get());
        }
    }

    @Test void staleAuthorityAndDatabaseFailuresRemainDistinct() throws Exception {
        for (boolean conflict : new boolean[]{true, false}) {
            try (var server = new SessionControlServer(new InetSocketAddress("127.0.0.1", 0), CREDENTIAL,
                received -> { if (conflict) throw new SessionCoordinator.Conflict("test stale");
                    throw new java.sql.SQLException("test unavailable"); })) {
                server.start();
                URI endpoint = URI.create("http://127.0.0.1:" + server.port());
                var client = new SessionControlClient(CREDENTIAL);
                if (conflict) assertThrows(SessionCoordinator.Conflict.class, () -> client.request(endpoint, command));
                else assertThrows(IOException.class, () -> client.request(endpoint, command));
            }
        }
    }

    @Test void oversizedRequestsAreRejectedBeforeDispatch() throws Exception {
        AtomicInteger executed = new AtomicInteger();
        try (var server = new SessionControlServer(new InetSocketAddress("127.0.0.1", 0), CREDENTIAL,
            received -> { executed.incrementAndGet(); return null; })) {
            server.start();
            var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + server.port() + "/canopy/session"))
                .header("Authorization", "Bearer " + CREDENTIAL)
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[8193])).build(),
                HttpResponse.BodyHandlers.discarding());
            assertEquals(413, response.statusCode());
            assertEquals(0, executed.get());
        }
    }

    @Test void oversizedResponsesFailWhileReceiving() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/canopy/session", exchange -> {
            try (exchange) {
                byte[] body = new byte[PlayerSession.MAX_SNAPSHOT_BYTES + 4096];
                exchange.sendResponseHeaders(200, body.length);
                try { exchange.getResponseBody().write(body); } catch (IOException cancelled) { }
            }
        });
        server.start();
        try {
            assertThrows(IOException.class, () -> new SessionControlClient(CREDENTIAL)
                .request(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), command));
        } finally { server.stop(0); }
    }

    @Test void stalledResponseBodyCannotHoldAnIoWorkerForever() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/canopy/session", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write(0);
                exchange.getResponseBody().flush();
                try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            }
        });
        server.start();
        long started = System.nanoTime();
        try {
            assertThrows(IOException.class, () -> new SessionControlClient(CREDENTIAL)
                .request(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), command));
            assertTrue(System.nanoTime() - started < java.time.Duration.ofSeconds(6).toNanos());
        } finally { release.countDown(); server.stop(0); }
    }
}
