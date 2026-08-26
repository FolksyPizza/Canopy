package com.folypizza.canopy;

import com.folypizza.canopy.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * HTTP metrics endpoint.
 * Responds to GET /canopy/metrics.json with per-region metrics.
 */
public class MetricsEndpoint {
    private static final Logger log = LoggerFactory.getLogger(MetricsEndpoint.class);
    private static final String URI_PREFIX = "/metrics";
    
    private final int port;
    private final MetricsCollector metricsCollector;
    private com.sun.net.httpserver.HttpServer server;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private boolean running = false;

    public MetricsEndpoint(int port, MetricsCollector metricsCollector) {
        this.port = port;
        this.metricsCollector = metricsCollector;
    }

    /**
     * Start the metrics endpoint.
     */
    public void start() {
        try {
            server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", exchange -> {
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
            log.info("Metrics HTTP endpoint started on port {}", port);
        } catch (IOException e) {
            log.error("Failed to start metrics HTTP server on port {}", port, e);
        }
    }

    /**
     * Stop the metrics endpoint.
     */
    public void stop() {
        if (server != null) {
            server.stop(0);
            running = false;
            executor.shutdown();
            log.info("Metrics HTTP endpoint stopped");
        }
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
        return server != null ? server.getAddress().getPort() : port;
    }
}
