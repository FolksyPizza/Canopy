package com.folksypizza.canopy.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Optional shard-to-shard authentication (grpc.shared-secret). Every call carries the secret; a server with a secret
 * rejects calls without it. Without it anyone who can reach the gRPC port could push player state (inventories) or
 * flood the coordination endpoints, so set it whenever the port is reachable beyond the shard hosts.
 */
public final class SharedSecret {
    private static final Metadata.Key<String> KEY = Metadata.Key.of("canopy-secret", Metadata.ASCII_STRING_MARSHALLER);

    private static volatile SharedSecret current = new SharedSecret("");

    /** Set once at startup, before any channel or server is built. */
    public static void install(SharedSecret secret) {
        current = secret;
    }

    public static SharedSecret current() {
        return current;
    }

    private final byte[] secret;

    public SharedSecret(String secret) {
        this.secret = secret == null ? new byte[0] : secret.getBytes(StandardCharsets.UTF_8);
    }

    public boolean enabled() {
        return secret.length > 0;
    }

    public ServerInterceptor server() {
        return new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                                                               ServerCallHandler<Q, R> next) {
                String presented = headers.get(KEY);
                byte[] given = presented == null ? new byte[0] : presented.getBytes(StandardCharsets.UTF_8);
                if (!MessageDigest.isEqual(given, secret)) {
                    call.close(Status.UNAUTHENTICATED.withDescription("bad shard secret"), new Metadata());
                    return new ServerCall.Listener<>() { };
                }
                return next.startCall(call, headers);
            }
        };
    }

    public ClientInterceptor client() {
        String value = new String(secret, StandardCharsets.UTF_8);
        return new ClientInterceptor() {
            @Override
            public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {
                    @Override
                    public void start(Listener<R> listener, Metadata headers) {
                        headers.put(KEY, value);
                        super.start(listener, headers);
                    }
                };
            }
        };
    }
}
