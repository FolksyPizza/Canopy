package com.folksypizza.canopy.session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Bounded asynchronous incident journal. Metadata only; this is not the durable gameplay transaction log. */
public final class SessionAudit implements AutoCloseable {
    public static final SessionAudit NONE = new SessionAudit();
    private static final Set<String> FIELDS = Set.of("player", "session", "gateway", "owner", "target", "transfer",
        "epoch", "revision", "sequence", "lifeRevision", "life", "phase", "operation", "request", "status",
        "durationMs", "snapshotBytes", "error", "cause", "sqlState", "reason", "count", "queued", "protocol",
        "x", "y", "z", "yaw", "pitch", "failureSite", "mode", "direction", "packetId", "packetBytes", "buffered", "chunks", "attempt", "world", "instance");
    private final String component;
    private final String run = UUID.randomUUID().toString();
    private final ArrayBlockingQueue<String> queue;
    private final Consumer<String> warning;
    private final Path directory;
    private final long maxBytes;
    private final int retained;
    private final AtomicLong missing = new AtomicLong();
    private final Thread writer;
    private FileChannel file;
    private volatile boolean closing;
    private long sequence, bytes, forcedAt;
    private long losses;

    private SessionAudit() {
        component = "disabled"; queue = null; warning = ignored -> {}; directory = null;
        maxBytes = 0; retained = 0; writer = null;
    }

    public SessionAudit(Path directory, String component, long maxBytes, int retained, int capacity,
                        Consumer<String> warning) throws IOException {
        if (maxBytes < 4096 || retained < 1 || retained > 100 || capacity < 1 || capacity > 65536)
            throw new IllegalArgumentException("Invalid audit limits");
        this.directory = directory; this.component = Objects.requireNonNull(component);
        this.maxBytes = maxBytes; this.retained = retained; this.warning = Objects.requireNonNull(warning);
        queue = new ArrayBlockingQueue<>(capacity);
        Files.createDirectories(directory);
        if (Files.isSymbolicLink(directory)) throw new IOException("Audit directory cannot be a symbolic link");
        permissions(directory, "rwx------");
        open();
        writer = new Thread(this::writeLoop, "canopy-audit-writer"); writer.setDaemon(true); writer.start();
        event("audit.start", Map.of());
    }

    public static SessionAudit configured(Path folder, String component, Properties config,
                                          Consumer<String> warning) throws IOException {
        if (!Boolean.parseBoolean(config.getProperty("audit.enabled", "true"))) return NONE;
        return new SessionAudit(folder.resolve("audit"), component,
            Long.parseLong(config.getProperty("audit.max-bytes", "16777216")),
            Integer.parseInt(config.getProperty("audit.retained-files", "8")),
            Integer.parseInt(config.getProperty("audit.queue-capacity", "4096")), warning);
    }

    /** Producers only enqueue bounded metadata; disk I/O and warnings belong to the writer. */
    public synchronized void event(String event, Map<String, ?> fields) {
        if (writer == null || closing) return;
        String line = json(event, ++sequence, fields);
        if (!queue.offer(line)) missing.incrementAndGet();
    }

    public void state(String event, PlayerSession state) { event(event, fields(state)); }

    public void failure(String event, Map<String, ?> fields, Throwable error) {
        Map<String, Object> safe = new LinkedHashMap<>(fields);
        // Exception messages can contain JDBC URLs, credentials, packet bodies or player text.
        safe.put("error", error.getClass().getName());
        Throwable cause = error;
        for (int i = 0; cause.getCause() != null && cause.getCause() != cause && i < 8; i++) cause = cause.getCause();
        safe.put("cause", cause.getClass().getName());
        if (cause.getStackTrace().length > 0) safe.put("failureSite", cause.getStackTrace()[0].toString());
        if (cause instanceof java.sql.SQLException sql) safe.put("sqlState", sql.getSQLState());
        event(event, safe);
    }

    public static Map<String, Object> fields(PlayerSession state) {
        Map<String, Object> fields = new LinkedHashMap<>();
        if (state == null) return fields;
        fields.put("player", state.playerId()); fields.put("session", state.sessionId());
        fields.put("gateway", state.gatewayId()); fields.put("owner", state.owner()); fields.put("target", state.target());
        fields.put("transfer", state.transferId()); fields.put("epoch", state.epoch()); fields.put("revision", state.revision());
        fields.put("sequence", state.snapshotSequence()); fields.put("lifeRevision", state.lifeRevision());
        fields.put("life", state.life()); fields.put("phase", state.phase()); fields.put("snapshotBytes", state.snapshotBytes());
        return fields;
    }

    public static Map<String, Object> fields(SessionCommand command) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("operation", command.operation()); fields.put("player", command.playerId());
        fields.put("session", command.sessionId()); fields.put("gateway", command.gatewayId());
        fields.put("owner", command.owner()); fields.put("target", command.target());
        fields.put("transfer", command.transferId()); fields.put("epoch", command.epoch());
        return fields;
    }

    private synchronized long nextSequence() { return ++sequence; }

    private String json(String event, long number, Map<String, ?> fields) {
        if (event == null || !event.matches("[a-z][a-z0-9_.]{0,95}")) throw new IllegalArgumentException("Invalid audit event");
        StringBuilder out = new StringBuilder("{\"schema\":1,\"time\":");
        quote(out, Instant.now().toString()); out.append(",\"run\":"); quote(out, run);
        out.append(",\"component\":"); quote(out, component);
        out.append(",\"auditSequence\":").append(number).append(",\"event\":"); quote(out, event);
        for (var field : fields.entrySet()) {
            if (!FIELDS.contains(field.getKey())) continue;
            Object value = field.getValue();
            if (!(value == null || value instanceof String || value instanceof UUID || value instanceof Enum<?>
                || value instanceof Boolean || value instanceof Integer || value instanceof Long
                || value instanceof Double d && Double.isFinite(d) || value instanceof Float f && Float.isFinite(f))) continue;
            out.append(','); quote(out, field.getKey()); out.append(':');
            if (value == null) out.append("null");
            else if (value instanceof Number || value instanceof Boolean) out.append(value);
            else quote(out, value.toString());
        }
        return out.append("}\n").toString();
    }

    private static void quote(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < Math.min(value.length(), 512); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') out.append('\\').append(c);
            else if (c < 32 || c >= 0xd800 && c <= 0xdfff) out.append(String.format("\\u%04x", (int) c));
            else out.append(c);
        }
        out.append('"');
    }

    private void writeLoop() {
        try {
            while (!closing || !queue.isEmpty() || missing.get() > 0) {
                String line = queue.poll(200, TimeUnit.MILLISECONDS);
                long dropped = missing.getAndSet(0);
                losses += dropped;
                if (dropped > 0) warn("Canopy audit records lost: " + dropped + "; inspect audit.gap after storage recovers");
                try {
                    if (file == null) open();
                    if (losses > 0) {
                        write(json("audit.gap", nextSequence(), Map.of("count", losses, "reason", "queue_storage_or_incomplete_tail")));
                        losses = 0;
                    }
                    if (line != null) write(line);
                    if (System.nanoTime() - forcedAt > TimeUnit.SECONDS.toNanos(1)) {
                        file.force(false); forcedAt = System.nanoTime();
                    }
                } catch (IOException unavailable) {
                    if (line != null) losses++;
                    warn("Canopy audit storage unavailable: " + unavailable.getClass().getSimpleName());
                    if (file != null) try { file.close(); } catch (IOException ignored) { }
                    file = null;
                    Thread.sleep(1000);
                }
            }
            if (losses > 0) warn("Canopy audit stopped with missing records: " + losses);
            if (file != null) file.force(false);
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        catch (IOException unavailable) { warn("Canopy audit final flush failed"); }
        finally { if (file != null) try { file.close(); } catch (IOException ignored) { } }
    }

    private void warn(String message) {
        try { warning.accept(message); } catch (RuntimeException ignored) { /* diagnostics must not stop the writer */ }
    }

    private void write(String line) throws IOException {
        byte[] encoded = line.getBytes(StandardCharsets.UTF_8);
        if (bytes > 0 && bytes + encoded.length > maxBytes) rotate();
        ByteBuffer data = ByteBuffer.wrap(encoded);
        while (data.hasRemaining()) file.write(data);
        bytes += encoded.length;
    }

    private void rotate() throws IOException {
        file.force(false); file.close(); file = null;
        for (int i = retained; i >= 1; i--) {
            Path source = directory.resolve(i == 1 ? "events.jsonl" : "events.jsonl." + (i - 1));
            Path target = directory.resolve("events.jsonl." + i);
            if (Files.exists(source, LinkOption.NOFOLLOW_LINKS)) Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
        open();
    }

    private void open() throws IOException {
        Path path = directory.resolve("events.jsonl");
        try {
            file = FileChannel.open(path, Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND, LinkOption.NOFOLLOW_LINKS), PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rw-------")));
        } catch (UnsupportedOperationException unsupported) {
            file = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND, LinkOption.NOFOLLOW_LINKS);
        }
        try {
            permissions(path, "rw-------"); bytes = file.size();
            if (bytes > 0) {
                // A crash can interrupt one append. Keep complete records and make the missing tail explicit.
                try (var read = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                    int size = (int) Math.min(bytes, 65536); ByteBuffer tail = ByteBuffer.allocate(size);
                    long start = bytes - size; read.position(start);
                    while (tail.hasRemaining() && read.read(tail) > 0) { }
                    if (tail.position() != size) throw new IOException("Audit tail could not be read");
                    if (tail.get(size - 1) != '\n') {
                        int end = size - 1; while (end >= 0 && tail.get(end) != '\n') end--;
                        long keep = end < 0 ? start : start + end + 1;
                        file.truncate(keep); bytes = keep; losses++;
                        warn("Canopy audit recovered an incomplete record; audit.gap will report the loss");
                    }
                }
            }
        } catch (IOException failed) { file.close(); file = null; throw failed; }
    }

    private static void permissions(Path path, String value) throws IOException {
        try { Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(value)); }
        catch (UnsupportedOperationException ignored) { /* platform ACLs apply on non-POSIX filesystems */ }
    }

    @Override public void close() {
        if (writer == null) return;
        synchronized (this) {
            if (closing) return;
            event("audit.stop", Map.of()); closing = true;
        }
        try { writer.join(5000); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        if (writer.isAlive()) warn("Canopy audit shutdown drain exceeded 5 seconds; queued records may be lost");
    }
}
