/* Copyright (C) 2026 Canopy Contributors
 * SPDX-License-Identifier: GPL-3.0-or-later */
package com.velocitypowered.proxy.connection.client;
import java.io.*;
import java.security.MessageDigest;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public record ChatStateFrame(int operation, UUID attempt, UUID player, String source, String destination,
    long expiresAt, int fragmentIndex, int fragmentCount, int totalLength, byte[] payload) {
  public static final String CHANNEL="canopy:chat_state";
  public static final int CAPTURE=1, RESTORE=2, ABORT=3, SNAPSHOT=11, RESTORED=12, REJECT=13;
  public static final int MAX_TOTAL=262144, MAX_FRAGMENT=24000;
  private static final int MAGIC=0x43434831;
  public byte[] encode(byte[] key)throws Exception {
    bounds();ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);
    out.writeInt(MAGIC);out.writeByte(1);out.writeByte(operation);uuid(out,attempt);uuid(out,player);
    out.writeUTF(source);out.writeUTF(destination);out.writeLong(expiresAt);out.writeShort(fragmentIndex);out.writeShort(fragmentCount);
    out.writeInt(totalLength);out.writeInt(payload.length);out.write(payload);out.flush();
    byte[] body=bytes.toByteArray();out.write(mac(key,body));return bytes.toByteArray();
  }
  public static ChatStateFrame decode(byte[] encoded,byte[] key,long now)throws Exception {
    if(encoded.length<90||encoded.length>MAX_FRAGMENT+256)throw new IOException("Invalid frame size");
    byte[] body=Arrays.copyOf(encoded,encoded.length-32),signature=Arrays.copyOfRange(encoded,encoded.length-32,encoded.length);
    if(!MessageDigest.isEqual(mac(key,body),signature))throw new IOException("Invalid frame authentication");
    DataInputStream in=new DataInputStream(new ByteArrayInputStream(body));
    if(in.readInt()!=MAGIC||in.readUnsignedByte()!=1)throw new IOException("Unknown frame schema");
    int op=in.readUnsignedByte();UUID attempt=uuid(in),player=uuid(in);String source=in.readUTF(),destination=in.readUTF();
    long expiry=in.readLong();int index=in.readUnsignedShort(),count=in.readUnsignedShort(),total=in.readInt(),length=in.readInt();
    if(length<0||length>MAX_FRAGMENT||in.available()!=length)throw new IOException("Invalid fragment length");
    ChatStateFrame frame=new ChatStateFrame(op,attempt,player,source,destination,expiry,index,count,total,in.readNBytes(length));frame.bounds();
    if(expiry<now||expiry>now+30000)throw new IOException("Expired or excessive handoff lifetime");return frame;
  }
  public List<ChatStateFrame> fragments(int op,byte[] bytes)throws IOException {
    if(bytes.length>MAX_TOTAL)throw new IOException("Snapshot exceeds bound");int count=Math.max(1,(bytes.length+MAX_FRAGMENT-1)/MAX_FRAGMENT);
    List<ChatStateFrame> result=new ArrayList<>();for(int i=0;i<count;i++)result.add(new ChatStateFrame(op,attempt,player,source,destination,expiresAt,i,count,bytes.length,Arrays.copyOfRange(bytes,i*MAX_FRAGMENT,Math.min(bytes.length,(i+1)*MAX_FRAGMENT))));return result;
  }
  public void bounds()throws IOException {
    if(!Set.of(1,2,3,11,12,13).contains(operation)||!source.matches("[a-zA-Z0-9_-]{1,64}")||!destination.matches("[a-zA-Z0-9_-]{1,64}")||source.equals(destination)
      ||fragmentCount<1||fragmentCount>12||fragmentIndex<0||fragmentIndex>=fragmentCount||totalLength<0||totalLength>MAX_TOTAL||payload.length>MAX_FRAGMENT
      ||fragmentCount!=Math.max(1,(totalLength+MAX_FRAGMENT-1)/MAX_FRAGMENT)||payload.length!=Math.min(MAX_FRAGMENT,totalLength-fragmentIndex*MAX_FRAGMENT))throw new IOException("Invalid frame bounds");
  }
  private static byte[] mac(byte[] key,byte[] value)throws Exception {
    if(key.length<32)throw new IllegalArgumentException("Weak bridge key");Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(key,"HmacSHA256"));return mac.doFinal(value);
  }
  static void uuid(DataOutputStream out,UUID value)throws IOException {out.writeLong(value.getMostSignificantBits());out.writeLong(value.getLeastSignificantBits());}
  static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
  public static final class Assembly {
    private final ChatStateFrame first;private final byte[][] pieces;private int received;
    public Assembly(ChatStateFrame frame)throws IOException {frame.bounds();first=frame;pieces=new byte[frame.fragmentCount()][];}
    public byte[] add(ChatStateFrame frame)throws IOException {
      if(frame.operation!=first.operation||!frame.attempt.equals(first.attempt)||!frame.player.equals(first.player)||!frame.source.equals(first.source)||!frame.destination.equals(first.destination)||frame.expiresAt!=first.expiresAt||frame.totalLength!=first.totalLength||frame.fragmentCount!=first.fragmentCount)throw new IOException("Mixed handoff fragments");
      if(pieces[frame.fragmentIndex]!=null)throw new IOException("Duplicate handoff fragment");pieces[frame.fragmentIndex]=frame.payload.clone();received++;
      if(received!=pieces.length)return null;ByteArrayOutputStream out=new ByteArrayOutputStream(first.totalLength);for(byte[] piece:pieces)out.write(piece);return out.toByteArray();
    }
  }
}
