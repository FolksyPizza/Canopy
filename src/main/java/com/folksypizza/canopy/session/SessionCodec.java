package com.folksypizza.canopy.session;

import java.io.*;
import java.sql.SQLException;
import java.util.UUID;

/** Bounded, versioned session representation shared by storage and the control channel. */
public final class SessionCodec {
    private static final int FORMAT = 2;
    private SessionCodec() {}
    public static byte[] encode(PlayerSession state) throws SQLException {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(FORMAT);
            out.writeUTF(state.playerId().toString());
            out.writeUTF(state.sessionId().toString());
            out.writeUTF(state.gatewayId());
            out.writeUTF(state.phase().name());
            writeNullable(out, state.owner());
            writeNullable(out, state.target());
            writeNullable(out, state.transferId() == null ? null : state.transferId().toString());
            out.writeLong(state.epoch());
            out.writeLong(state.revision());
            out.writeLong(state.snapshotSequence());
            byte[] snapshot = state.snapshot();
            out.writeInt(snapshot.length);
            out.write(snapshot);
            out.writeLong(state.lifeRevision());
            out.writeUTF(state.life().name());
            return bytes.toByteArray();
        } catch (IOException e) { throw new SQLException("Cannot encode session state", e); }
    }

    public static PlayerSession decode(byte[] bytes) throws SQLException {
        if (bytes == null || bytes.length > PlayerSession.MAX_SNAPSHOT_BYTES + 2048) {
            throw new SQLException("Invalid session payload size");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int format = in.readInt();
            if (format != 1 && format != FORMAT) throw new IOException("Unsupported session format");
            UUID player = UUID.fromString(in.readUTF());
            UUID session = UUID.fromString(in.readUTF());
            String gateway = in.readUTF();
            PlayerSession.Phase phase = PlayerSession.Phase.valueOf(in.readUTF());
            String owner = readNullable(in), target = readNullable(in), transfer = readNullable(in);
            long epoch = in.readLong(), revision = in.readLong(), sequence = in.readLong();
            int length = in.readInt();
            if (length < 0 || length > PlayerSession.MAX_SNAPSHOT_BYTES || length > in.available()) {
                throw new IOException("Invalid snapshot length");
            }
            byte[] snapshot = new byte[length];
            in.readFully(snapshot);
            long lifeRevision = format == 1 ? 0 : in.readLong();
            PlayerSession.Life life = format == 1 ? PlayerSession.Life.ALIVE : PlayerSession.Life.valueOf(in.readUTF());
            if (in.available() != 0) throw new IOException("Trailing session bytes");
            return new PlayerSession(player, session, gateway, phase, owner, target,
                transfer == null ? null : UUID.fromString(transfer), epoch, revision, sequence, snapshot, lifeRevision, life);
        } catch (IOException | IllegalArgumentException | NullPointerException e) {
            throw new SQLException("Invalid stored session state", e);
        }
    }

    private static void writeNullable(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) out.writeUTF(value);
    }

    private static String readNullable(DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
