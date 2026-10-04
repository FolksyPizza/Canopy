package com.folksypizza.canopy.session;

import java.io.*;

/** Dimension and routing context around the native player codec's opaque payload. */
public record SessionSnapshot(String world, String shard, byte[] playerState, SpawnReference spawn) {
    public SessionSnapshot(String world, String shard, byte[] playerState) { this(world, shard, playerState, null); }
    public SessionSnapshot {
        PlayerSession.requireName(world); PlayerSession.requireName(shard);
        if (playerState == null || playerState.length == 0 || playerState.length > PlayerSession.MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("Invalid player snapshot");
        }
        playerState = playerState.clone();
    }
    @Override public byte[] playerState() { return playerState.clone(); }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(2); out.writeUTF(world); out.writeUTF(shard); out.writeInt(playerState.length); out.write(playerState);
        out.writeBoolean(spawn != null); if (spawn != null) spawn.write(out);
        byte[] encoded = bytes.toByteArray();
        if (encoded.length > PlayerSession.MAX_SNAPSHOT_BYTES) throw new IOException("Player snapshot too large");
        return encoded;
    }

    public static SessionSnapshot decode(byte[] bytes) throws IOException {
        try {
            if (bytes.length > PlayerSession.MAX_SNAPSHOT_BYTES) throw new IOException("Player snapshot too large");
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int version = in.readInt();
            if (version != 1 && version != 2) throw new IOException("Unknown player snapshot format");
            String world = in.readUTF(), shard = in.readUTF();
            int length = in.readInt();
            if (length < 1 || length > in.available()) throw new IOException("Invalid player snapshot length");
            byte[] state = in.readNBytes(length);
            SpawnReference spawn = version == 2 && in.readBoolean() ? SpawnReference.read(in) : null;
            if (in.available() != 0) throw new IOException("Trailing player snapshot bytes");
            return new SessionSnapshot(world, shard, state, spawn);
        } catch (IllegalArgumentException e) { throw new IOException("Invalid player snapshot", e); }
    }
}
