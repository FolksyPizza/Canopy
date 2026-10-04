package com.folksypizza.canopy.session;

import java.io.*;
import java.util.UUID;

/** Commands travel independently of player sockets. Transport authenticates them before decoding. */
public record SessionCommand(Operation operation, UUID playerId, UUID sessionId, String gatewayId,
                             String owner, String target, UUID transferId, long epoch) {
    public enum Operation { OPEN, FIND, ATTACH, PREPARE, ROUTE_RESPAWN, COMMIT, ACTIVATE, DISCONNECT, RECOVER_LOGOUT, CANCEL, HEARTBEAT }

    public PlayerSession.GatewayToken token() {
        return new PlayerSession.GatewayToken(playerId, sessionId, gatewayId);
    }

    public static SessionCommand gateway(Operation operation, PlayerSession.GatewayToken token) {
        return new SessionCommand(operation, token.playerId(), token.sessionId(), token.gatewayId(), null, null, null, 0);
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(1);
        out.writeUTF(operation.name());
        write(out, playerId == null ? null : playerId.toString());
        write(out, sessionId == null ? null : sessionId.toString());
        write(out, gatewayId); write(out, owner); write(out, target);
        write(out, transferId == null ? null : transferId.toString());
        out.writeLong(epoch);
        return bytes.toByteArray();
    }

    public static SessionCommand decode(byte[] bytes) throws IOException {
        if (bytes.length > 8192) throw new IOException("Session command too large");
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != 1) throw new IOException("Unknown session command format");
            Operation operation = Operation.valueOf(in.readUTF());
            String player = read(in), session = read(in), gateway = read(in), owner = read(in), target = read(in), transfer = read(in);
            long epoch = in.readLong();
            if (in.available() != 0) throw new IOException("Trailing session command bytes");
            return new SessionCommand(operation, uuid(player), uuid(session), gateway, owner, target, uuid(transfer), epoch);
        } catch (IllegalArgumentException e) { throw new IOException("Invalid session command", e); }
    }

    private static UUID uuid(String value) { return value == null ? null : UUID.fromString(value); }
    private static void write(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) { PlayerSession.requireName(value); out.writeUTF(value); }
    }
    private static String read(DataInputStream in) throws IOException {
        String value = in.readBoolean() ? in.readUTF() : null;
        if (value != null) PlayerSession.requireName(value);
        return value;
    }
}
