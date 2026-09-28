package com.folypizza.canopy.repartition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

final class RepartitionJsonCodec {
    static final ObjectMapper MAPPER = new ObjectMapper();

    private RepartitionJsonCodec() {}

    static String transaction(RepartitionTransaction tx) {
        try {
            return MAPPER.writeValueAsString(tx);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize repartition transaction", e);
        }
    }

    static RepartitionTransaction transaction(String json) {
        try {
            return MAPPER.readValue(json, RepartitionTransaction.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not deserialize repartition transaction", e);
        }
    }

    static String ownership(RepartitionStateStore.Ownership ownership) {
        try {
            return MAPPER.writeValueAsString(ownership);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize repartition ownership", e);
        }
    }

    static RepartitionStateStore.Ownership ownership(String json) {
        try {
            return MAPPER.readValue(json, RepartitionStateStore.Ownership.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not deserialize repartition ownership", e);
        }
    }
}
