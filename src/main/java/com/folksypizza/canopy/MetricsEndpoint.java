package com.folksypizza.canopy;

import com.folksypizza.canopy.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * HTTP metrics endpoint.
 * Responds to GET /canopy/metrics.json with per-region metrics.
 */
public class MetricsEndpoint {
    private static final Logger log = LoggerFactory.getLogger(MetricsEndpoint.class);
    private final boolean enabled;
    private final InetSocketAddress bindAddress;
    private final MetricsCollector metricsCollector;
    private com.sun.net.httpserver.HttpServer server;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private boolean running = false;

    public MetricsEndpoint(boolean enabled, InetSocketAddress bindAddress, MetricsCollector metricsCollector) {
        this.enabled = enabled;
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
        this.metricsCollector = Objects.requireNonNull(metricsCollector, "metricsCollector");
    }

    /**
     * Start the metrics endpoint.
     */
    public void start() {
        if (!this.enabled) {
            log.info("Metrics HTTP endpoint disabled");
            return;
        }
        try {
            server = com.sun.net.httpserver.HttpServer.create(this.bindAddress, 0);
            server.createContext("/canopy/metrics.json", exchange -> {
                String path = exchange.getRequestURI().getPath();
                if ("/canopy/metrics.json".equals(path)) {
                    String metricsJson;
                    try {
                        metricsJson = metricsCollector.getMetricsSnapshot();
                    } catch (Exception e) {
                        exchange.sendResponseHeaders(500, 0);
                        exchange.close();
                        return;
                    }
                    byte[] bytes = metricsJson.getBytes("UTF-8");
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                } else {
                    exchange.sendResponseHeaders(404, 0);
                }
                exchange.close();
            });
            server.setExecutor(executor);
            server.start();
            running = true;
            log.info("Metrics HTTP endpoint started on {}", this.server.getAddress());
        } catch (IOException e) {
            log.error("Failed to start metrics HTTP server on {}", this.bindAddress, e);
        }
    }

    /**
     * Stop the metrics endpoint.
     */
    public void stop() {
        if (server != null) {
            server.stop(0);
            running = false;
            log.info("Metrics HTTP endpoint stopped");
        }
        executor.shutdownNow();
    }

    /**
     * Check if this endpoint is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Get the port this server is bound to.
     */
    public int getPort() {
        return server != null ? server.getAddress().getPort() : this.bindAddress.getPort();
    }
}
