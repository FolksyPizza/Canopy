/*
 * Copyright (C) 2018-2025 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.velocitypowered.proxy.connection.client;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Fail-closed source, target, transfer and timeout gate for experimental shadow promotion. */
final class CanopyShadowPromotionGate {

  static final long TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(3_000L);

  private enum State { IDLE, ARMED, ACKNOWLEDGED }

  private State state = State.IDLE;
  private Object source;
  private String target;
  private UUID transferId;
  private long armedAtNanos;

  synchronized boolean arm(Object source, String target, UUID transferId, long nowNanos) {
    if (state != State.IDLE || source == null || target == null || target.isBlank() || transferId == null) {
      return false;
    }
    this.source = source;
    this.target = target.toLowerCase(Locale.ROOT);
    this.transferId = transferId;
    this.armedAtNanos = nowNanos;
    this.state = State.ARMED;
    return true;
  }

  synchronized boolean acknowledge(Object source, String target, UUID transferId, long nowNanos) {
    if (state != State.ARMED) return false;
    if (expired(nowNanos)) {
      clear();
      return false;
    }
    if (this.source != source || !targetMatches(target) || !this.transferId.equals(transferId)) return false;
    state = State.ACKNOWLEDGED;
    return true;
  }

  synchronized boolean mayPromote() {
    return state == State.ACKNOWLEDGED;
  }

  /** Consume the positive acknowledgement exactly once before changing the serving connection. */
  synchronized boolean consumeAcknowledgement() {
    if (state != State.ACKNOWLEDGED) return false;
    clear();
    return true;
  }

  synchronized boolean isPending() {
    return state != State.IDLE;
  }

  synchronized boolean expired(long nowNanos) {
    return state != State.IDLE && nowNanos - armedAtNanos >= TIMEOUT_NANOS;
  }

  synchronized Object source() {
    return source;
  }

  synchronized boolean targetMatches(String candidate) {
    return candidate != null && target != null && target.equals(candidate.toLowerCase(Locale.ROOT));
  }

  synchronized void clear() {
    state = State.IDLE;
    source = null;
    target = null;
    transferId = null;
    armedAtNanos = 0L;
  }
}
