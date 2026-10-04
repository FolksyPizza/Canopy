package com.folksypizza.canopy.session;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionAuditTest {
    @TempDir Path directory;
    private static final ObjectMapper JSON = new ObjectMapper();

    private List<JsonNode> records(Path path) throws Exception {
        var records = new ArrayList<JsonNode>();
        try (var files = Files.list(path)) {
            for (Path file : files.filter(p -> p.getFileName().toString().startsWith("events.jsonl")).toList())
                for (String line : Files.readAllLines(file)) records.add(JSON.readTree(line));
        }
        return records;
    }

    @Test void concurrentEventsDrainAndCorrelateWithoutSerializingSecretsOrPayloads() throws Exception {
        UUID player = UUID.randomUUID(), session = UUID.randomUUID();
        var audit = new SessionAudit(directory, "test", 1 << 20, 2, 4096, ignored -> {});
        var pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> work = new ArrayList<>();
            for (int worker = 0; worker < 4; worker++) work.add(pool.submit(() -> {
                for (int i = 0; i < 100; i++) audit.event("test.event", Map.of("player", player, "session", session,
                    "reason", "quote\"\n\\\u0000", "password", "synthetic-do-not-log", "snapshotBytes", new byte[]{1, 2}));
            }));
            for (var task : work) task.get();
            audit.failure("test.failure", Map.of(), new java.sql.SQLException("synthetic-do-not-log", "08006"));
        } finally { pool.shutdownNow(); audit.close(); }
        var rows = records(directory);
        assertEquals(403, rows.size());
        assertEquals(403, rows.stream().map(r -> r.get("auditSequence").asLong()).distinct().count());
        assertEquals(1, rows.stream().map(r -> r.get("run").asText()).distinct().count());
        for (var row : rows) assertFalse(row.toString().contains("synthetic-do-not-log"));
        var data = rows.stream().filter(r -> r.get("event").asText().equals("test.event")).findFirst().orElseThrow();
        assertEquals(player.toString(), data.get("player").asText());
        assertEquals(session.toString(), data.get("session").asText());
        assertEquals("quote\"\n\\\u0000", data.get("reason").asText());
        assertFalse(data.has("password")); assertFalse(data.has("snapshotBytes"));
        assertTrue(rows.stream().anyMatch(r -> r.path("sqlState").asText().equals("08006")));
        audit.event("test.after_close", Map.of());
        assertEquals(403, records(directory).size());
    }

    @Test void rotationBoundsRetentionAndRestartAppendsWithANewRun() throws Exception {
        try (var audit = new SessionAudit(directory, "test", 4096, 2, 4096, ignored -> {})) {
            for (int i = 0; i < 80; i++) audit.event("test.event", Map.of("reason", "x".repeat(512)));
        }
        try (var files = Files.list(directory)) { assertEquals(3, files.count()); }
        var before = records(directory);
        assertFalse(before.isEmpty());
        String oldRun = before.getLast().path("run").asText();
        try (var audit = new SessionAudit(directory, "test", 4096, 2, 10, ignored -> {})) {
            audit.event("test.restart", Map.of());
        }
        var after = records(directory);
        assertTrue(after.stream().anyMatch(r -> r.path("run").asText().equals(oldRun)));
        assertTrue(after.stream().anyMatch(r -> r.path("event").asText().equals("test.restart")
            && !r.path("run").asText().equals(oldRun)));
        assertEquals("rw-------", java.nio.file.attribute.PosixFilePermissions.toString(
            Files.getPosixFilePermissions(directory.resolve("events.jsonl"))));
    }

    @Test void saturationIsExplicitAndDoesNotBlockProducers() throws Exception {
        var warnings = new CopyOnWriteArrayList<String>();
        long start = System.nanoTime();
        try (var audit = new SessionAudit(directory, "test", 1 << 24, 2, 1, warnings::add)) {
            for (int i = 0; i < 5000; i++) audit.event("test.event", Map.of("count", i));
        }
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(8));
        assertFalse(warnings.isEmpty());
        var rows = records(directory);
        assertEquals(rows.size(), rows.stream().map(r -> r.path("auditSequence").asLong()).distinct().count());
        assertTrue(records(directory).stream().anyMatch(r -> r.path("event").asText().equals("audit.gap")
            && r.path("count").asLong() > 0));
    }

    @Test void controlDenialsAndDatabaseErrorsAreAuditedWithoutCredentialOrExceptionMessage() throws Exception {
        String secret = String.join("", "synthetic-test-control-", "credential");
        var command = new SessionCommand(SessionCommand.Operation.OPEN, UUID.randomUUID(), UUID.randomUUID(),
            "test-gateway", null, null, null, 0);
        try (var audit = new SessionAudit(directory, "test", 1 << 20, 2, 100, ignored -> {});
             var server = new SessionControlServer(new InetSocketAddress("127.0.0.1", 0), secret,
                 received -> { throw new java.sql.SQLException(secret, "08006"); }, audit)) {
            server.start(); URI uri = URI.create("http://127.0.0.1:" + server.port());
            assertThrows(java.io.IOException.class, () -> new SessionControlClient("wrong-test-credential", audit).request(uri, command));
            assertThrows(java.io.IOException.class, () -> new SessionControlClient(secret, audit).request(uri, command));
        }
        var rows = records(directory);
        assertTrue(rows.stream().anyMatch(r -> r.path("status").asInt() == 401));
        assertTrue(rows.stream().anyMatch(r -> r.path("status").asInt() == 503));
        assertEquals(2, rows.stream().filter(r -> r.path("event").asText().equals("control.response")).count());
        var sent = rows.stream().filter(r -> r.path("event").asText().equals("control.send")).toList();
        for (var send : sent) assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals("control.completed")
            && r.path("request").equals(send.path("request"))));
        for (var row : rows) { assertFalse(row.toString().contains(secret)); assertFalse(row.toString().contains("wrong-test-credential")); }
    }

    @Test void allDurableTransitionsRecordTheCommittedRevisionAndLife() throws Exception {
        try (var audit = new SessionAudit(directory, "test", 1 << 20, 2, 100, ignored -> {})) {
            var coordinator = new SessionCoordinator(new MemorySessionStoreTest().createStore(), audit);
            var state = coordinator.open(UUID.randomUUID(), UUID.randomUUID(), "gateway");
            state = coordinator.attach(state.gatewayToken(), "alpha");
            state = coordinator.checkpoint(state.authorityToken(), 1, new byte[]{1});
            state = coordinator.death(state.authorityToken(), 2, new byte[]{2});
            state = coordinator.respawn(state.authorityToken(), state.lifeRevision(), 1, new byte[]{3});
            state = coordinator.prepareHandover(state.authorityToken(), UUID.randomUUID(), "beta", 1, new byte[]{4});
            state = coordinator.commitHandover(state.gatewayToken(), state.transferId(), state.epoch());
            state = coordinator.disconnect(state.gatewayToken());
            coordinator.finishLogout(state.authorityToken(), 1, new byte[]{5});
        }
        var rows = records(directory);
        for (String event : List.of("session.open", "session.attach", "snapshot.checkpoint", "life.death", "life.respawn",
            "handoff.prepare", "handoff.commit", "session.disconnect", "session.logout"))
            assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals(event)), event);
        var dead = rows.stream().filter(r -> r.path("event").asText().equals("life.death")).findFirst().orElseThrow();
        assertEquals("DEAD", dead.path("life").asText()); assertEquals(1, dead.path("lifeRevision").asLong());
        assertEquals(4, dead.path("revision").asLong());
    }

    @Test void symlinksCannotRedirectAuditWritesAndDisabledLoggingCreatesNothing() throws Exception {
        Path outside = directory.resolve("outside"); Files.writeString(outside, "retained");
        Path logs = directory.resolve("logs"); Files.createDirectory(logs);
        Files.createSymbolicLink(logs.resolve("events.jsonl"), outside);
        assertThrows(java.io.IOException.class, () -> new SessionAudit(logs, "test", 4096, 2, 10, ignored -> {}));
        assertEquals("retained", Files.readString(outside));
        var config = new Properties(); config.setProperty("audit.enabled", "false");
        assertSame(SessionAudit.NONE, SessionAudit.configured(directory.resolve("disabled"), "test", config, ignored -> {}));
        assertFalse(Files.exists(directory.resolve("disabled")));
    }
    @Test void unavailableStorageReportsLossAndWritesAGapAfterRecovery() throws Exception {
        Path logs = directory.resolve("logs"), moved = directory.resolve("moved");
        CountDownLatch unavailable = new CountDownLatch(1);
        try (var audit = new SessionAudit(logs, "test", 4096, 2, 200, message -> {
            if (message.contains("storage unavailable")) unavailable.countDown();
        })) {
            Files.move(logs, moved);
            for (int i = 0; i < 8; i++) audit.event("test.before_recovery", Map.of("reason", "x".repeat(512)));
            assertTrue(unavailable.await(5, TimeUnit.SECONDS));
            Files.move(moved, logs);
            audit.event("test.recovered", Map.of());
        } finally {
            if (Files.exists(moved) && !Files.exists(logs)) Files.move(moved, logs);
        }
        var rows = records(logs);
        assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals("audit.gap")));
        assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals("test.recovered")));
    }

    @Test void restartKeepsCompleteLinesAndReportsAnInterruptedAppend() throws Exception {
        Files.writeString(directory.resolve("events.jsonl"), "{\"event\":\"retained\"}\n{\"event\":");
        var warnings = new ArrayList<String>();
        try (var audit = new SessionAudit(directory, "test", 4096, 2, 100, warnings::add)) {
            audit.event("test.after_restart", Map.of());
        }
        var rows = records(directory);
        assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals("retained")));
        assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals("audit.gap")));
        assertTrue(rows.stream().anyMatch(r -> r.path("event").asText().equals("test.after_restart")));
        assertFalse(warnings.isEmpty());
    }

}
