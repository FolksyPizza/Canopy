package com.folypizza.canopy.repartition;

import java.util.EnumSet;
import java.util.Set;

/**
 * Explicit lifecycle for a runtime repartition transaction.
 *
 * The ownership commit is the only irreversible transition. States before COMMITTED may be
 * aborted; states at/after COMMITTED may only move forward or remain failed for recovery.
 */
public enum RepartitionPhase {
    REQUESTED,
    PREPARING,
    QUIESCING,
    SNAPSHOTTING,
    TRANSFERRING,
    DESTINATION_READY,
    COMMITTING,
    COMMITTED,
    CLEANING_UP,
    COMPLETE,
    ABORTING,
    ABORTED,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETE || this == ABORTED;
    }

    public boolean isCommittedOrLater() {
        return this == COMMITTED || this == CLEANING_UP || this == COMPLETE;
    }

    public boolean isAbortable() {
        return switch (this) {
            case REQUESTED, PREPARING, QUIESCING, SNAPSHOTTING, TRANSFERRING,
                 DESTINATION_READY, ABORTING -> true;
            default -> false;
        };
    }

    public boolean canTransitionTo(RepartitionPhase next) {
        if (next == this) return true;
        if (isTerminal()) return false;
        if (next == FAILED) return true;
        return switch (this) {
            case REQUESTED -> next == PREPARING || next == ABORTING;
            case PREPARING -> next == QUIESCING || next == ABORTING;
            case QUIESCING -> next == SNAPSHOTTING || next == ABORTING;
            case SNAPSHOTTING -> next == TRANSFERRING || next == ABORTING;
            case TRANSFERRING -> next == DESTINATION_READY || next == ABORTING;
            case DESTINATION_READY -> next == COMMITTING || next == ABORTING;
            case COMMITTING -> next == COMMITTED;
            case COMMITTED -> next == CLEANING_UP;
            case CLEANING_UP -> next == COMPLETE;
            case ABORTING -> next == ABORTED;
            case FAILED -> false;
            case COMPLETE, ABORTED -> false;
        };
    }

    public void requireTransition(RepartitionPhase next) {
        if (!canTransitionTo(next)) {
            throw new IllegalStateException("Invalid repartition transition " + this + " -> " + next);
        }
    }
}
