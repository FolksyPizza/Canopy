package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.*;

import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class CanopyArrivalViewTest {
  @Test void nativeRepairChunkUnloadsAreHiddenUntilReadyButOrdinaryUnloadsPass() {
    var profile = CanopyProtocolProfile.find(774);
    var view = new CanopyArrivalView();
    ByteBuf forget = packet(profile, 0x25).writeLong(0);
    ByteBuf respawn = packet(profile, 0x50);
    ProtocolUtils.writeVarInt(respawn, 3);
    ProtocolUtils.writeString(respawn, "world");
    try {
      assertFalse(view.consumeRepair(profile, forget, (type, name) -> true));
      view.armRepair();
      assertFalse(view.consumeRepair(profile, forget, (type, name) -> true));
      assertTrue(view.consumeRepair(profile, respawn, (type, name) -> true));
      assertTrue(view.consumeRepair(profile, forget, (type, name) -> true));
      assertEquals(0, forget.readerIndex());
      assertTrue(view.drain().isEmpty());
      assertFalse(view.consumeRepair(profile, forget, (type, name) -> true));
    } finally { forget.release(); respawn.release(); view.close(); }
  }
  @Test void vitalsUseTheirActualWireIdsOnEverySupportedInternalProtocol() {
    for (int version = 771; version <= 777; version++) {
      var profile = CanopyProtocolProfile.find(version);
      var view = new CanopyArrivalView();
      for (int canonical : new int[]{0x65, 0x66, 0x67}) {
        ByteBuf buf = packet(profile, canonical);
        try { assertTrue(view.hold(profile, buf)); } finally { buf.release(); }
      }
      var held = view.drain();
      try { assertEquals(3, held.size()); } finally { held.forEach(ByteBuf::release); view.close(); }
    }
  }
  private ByteBuf packet(CanopyProtocolProfile profile, int canonical) {
    ByteBuf buf = Unpooled.buffer();
    ProtocolUtils.writeVarInt(buf, profile.clientboundWire(canonical));
    return buf;
  }

  @Test void ordinaryRespawnsPassWithoutAnExplicitRepairMarkerForEveryProfile() {
    for (int version = 771; version <= 777; version++) {
      var profile = CanopyProtocolProfile.find(version);
      var view = new CanopyArrivalView();
      ByteBuf buf = packet(profile, 0x50);
      try { assertFalse(view.consumeRepair(profile, buf, (type, name) -> true)); }
      finally { buf.release(); view.close(); }
    }
  }

  @Test void sameWorldRepairIsOneShotAndDoesNotChangeTheBufferIndex() {
    for (int version = 771; version <= 777; version++) {
      var profile = CanopyProtocolProfile.find(version);
      var view = new CanopyArrivalView();
      ByteBuf buf = packet(profile, 0x50);
      ProtocolUtils.writeVarInt(buf, 3);
      ProtocolUtils.writeString(buf, "world");
      try {
        view.armRepair();
        assertTrue(view.consumeRepair(profile, buf, (type, name) -> type == 3 && name.equals("world")));
        assertEquals(0, buf.readerIndex());
        assertFalse(view.consumeRepair(profile, buf, (type, name) -> true));
      } finally { buf.release(); view.close(); }
    }
  }

  @Test void dimensionMismatchAndMalformedRepairCannotHideALaterGenuineRespawn() {
    var profile = CanopyProtocolProfile.find(774);
    var view = new CanopyArrivalView();
    ByteBuf buf = packet(profile, 0x50);
    try {
      view.armRepair();
      assertFalse(view.consumeRepair(profile, buf, (type, name) -> true));
      ProtocolUtils.writeVarInt(buf, 3);
      ProtocolUtils.writeString(buf, "other");
      assertFalse(view.consumeRepair(profile, buf, (type, name) -> true));
      view.armRepair();
      assertFalse(view.consumeRepair(profile, buf, (type, name) -> name.equals("world")));
      assertFalse(view.consumeRepair(profile, buf, (type, name) -> true));
    } finally { buf.release(); view.close(); }
  }

  @Test void staleDeathIsHiddenAndOnlyLatestHealthIsReleasedOnReady() {
    var profile = CanopyProtocolProfile.find(774);
    var view = new CanopyArrivalView();
    ByteBuf dead = packet(profile, 0x66).writeFloat(0);
    ByteBuf alive = packet(profile, 0x66).writeFloat(7);
    ByteBuf deathScreen = packet(profile, 0x42);
    try {
      assertTrue(view.hold(profile, dead));
      assertTrue(view.hold(profile, deathScreen));
      assertTrue(view.hold(profile, alive));
      var released = view.drain();
      try {
        assertEquals(1, released.size());
        assertEquals(0x66, ProtocolUtils.readVarInt(released.getFirst()));
        assertEquals(7, released.getFirst().readFloat());
      } finally { released.forEach(ByteBuf::release); }
      assertTrue(view.drain().isEmpty());
    } finally { dead.release(); alive.release(); deathScreen.release(); view.close(); }
  }

  @Test void cancellationDiscardsHeldVitalsAndDisarmsRepair() {
    var profile = CanopyProtocolProfile.find(774);
    var view = new CanopyArrivalView();
    ByteBuf health = packet(profile, 0x66).writeFloat(0);
    ByteBuf respawn = packet(profile, 0x50);
    ProtocolUtils.writeVarInt(respawn, 0);
    ProtocolUtils.writeString(respawn, "world");
    try {
      view.hold(profile, health); view.armRepair(); view.close();
      assertTrue(view.drain().isEmpty());
      assertFalse(view.consumeRepair(profile, respawn, (type, name) -> true));
      assertEquals(1, health.refCnt());
    } finally { health.release(); respawn.release(); }
  }
}
