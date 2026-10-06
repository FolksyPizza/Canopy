package com.folksypizza.canopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.folksypizza.canopy.metrics.MetricsCollector;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class MetricsEndpointTest {
    @Test
    void servesMetricsOnTheConfiguredLoopbackAddress() throws Exception {
        MetricsEndpoint endpoint = new MetricsEndpoint(true,
            new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), new MetricsCollector(23));
        endpoint.start();
        try {
            assertTrue(endpoint.isRunning());
            HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + endpoint.getPort() + "/canopy/metrics.json")).GET().build();
            HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, response.statusCode());
            assertEquals(23, new ObjectMapper().readTree(response.body()).get("shard_id").asLong());
        } finally {
            endpoint.stop();
        }
        assertFalse(endpoint.isRunning());
    }

    @Test
    void disabledMetricsEndpointDoesNotStart() {
        MetricsEndpoint endpoint = new MetricsEndpoint(false,
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), new MetricsCollector(1));

        endpoint.start();

        assertFalse(endpoint.isRunning());
        assertEquals(0, endpoint.getPort());
        endpoint.stop();
    }
}
