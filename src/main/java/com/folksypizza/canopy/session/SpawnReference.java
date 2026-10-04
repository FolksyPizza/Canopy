package com.folksypizza.canopy.session;

import java.io.*;

/** Raw global spawn block; only its simulation owner may validate or consume it. */
public record SpawnReference(String world, int x, int y, int z, float yaw, float pitch, boolean forced) {
    public SpawnReference {
        PlayerSession.requireName(world);
        if (!Float.isFinite(yaw) || !Float.isFinite(pitch)) throw new IllegalArgumentException("Invalid spawn view");
    }
    void write(DataOutputStream out) throws IOException {
        out.writeUTF(world); out.writeInt(x); out.writeInt(y); out.writeInt(z);
        out.writeFloat(yaw); out.writeFloat(pitch); out.writeBoolean(forced);
    }
    static SpawnReference read(DataInputStream in) throws IOException {
        return new SpawnReference(in.readUTF(), in.readInt(), in.readInt(), in.readInt(), in.readFloat(), in.readFloat(), in.readBoolean());
    }
}
