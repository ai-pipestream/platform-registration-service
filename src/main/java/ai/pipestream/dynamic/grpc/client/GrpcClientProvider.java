package ai.pipestream.dynamic.grpc.client;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import ai.pipestream.dynamic.grpc.client.discovery.ServiceDiscovery;
import io.quarkus.cache.CacheResult;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Generic gRPC client provider that supports any Mutiny stub type.
 * This provider uses reflection to create stubs dynamically, allowing
 * for type-safe client creation without compile-time dependencies.
 */
/**
 * Provides creation and lifecycle management of gRPC Mutiny stubs and channels.
 */
@ApplicationScoped
public class GrpcClientProvider {
    
    /**
     * Default constructor for CDI.
     */
    public GrpcClientProvider() { }
    
    private static final Logger LOG = LoggerFactory.getLogger(GrpcClientProvider.class);
    private static final String CHANNEL_CACHE_NAME = "grpc-channels";
    
    @Inject
    ServiceDiscovery serviceDiscovery;
    
    // gRPC performance configuration - use Quarkus standard properties
    // Read from quarkus.grpc.clients."*".* properties (wildcard client config)
    // These match the Quarkus gRPC client configuration properties per the official docs:
    // https://quarkus.io/guides/grpc-service-consumption
    private int getFlowControlWindow() {
        Config config = ConfigProvider.getConfig();
        return config.getOptionalValue("quarkus.grpc.clients.\"*\".flow-control-window", Integer.class)
                .orElse(104857600);  // 100MB default
    }
    
    private int getMaxInboundMessageSize() {
        Config config = ConfigProvider.getConfig();
        return config.getOptionalValue("quarkus.grpc.clients.\"*\".max-inbound-message-size", Integer.class)
                .orElse(2147483647);  // 2GB - 1 byte
    }
    
    private int getMaxOutboundMessageSize() {
        Config config = ConfigProvider.getConfig();
        return config.getOptionalValue("quarkus.grpc.clients.\"*\".max-outbound-message-size", Integer.class)
                .orElse(2147483647);  // 2GB - 1 byte
    }
    
    // Cache of channels keyed by "host:port"
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    
    /**
     * Get a Mutiny stub client for a specific host and port.
     * 
     * @param stubClass The Mutiny stub class (e.g., MutinyPipeStepProcessorServiceGrpc.MutinyPipeStepProcessorStub)
     * @param host The target host
     * @param port The target port
     * @param <T> The stub type
     * @return The Mutiny stub instance
     */
    public <T> T getClient(Class<T> stubClass, String host, int port) {
        String key = host + ":" + port;
        
        ManagedChannel channel = channels.computeIfAbsent(key, k -> {
            int flowControlWindow = getFlowControlWindow();
            int maxInboundMessageSize = getMaxInboundMessageSize();
            int maxOutboundMessageSize = getMaxOutboundMessageSize();
            
            LOG.info("Creating new gRPC channel for {} with flow control window: {} bytes ({} MB)", 
                    key, flowControlWindow, flowControlWindow / (1024 * 1024));
            
            // Use NettyChannelBuilder to set HTTP/2 initial flow control window
            // This is CRITICAL for large message performance - the default 64KB window
            // causes severe throughput bottlenecks (5-10 MB/s instead of 100+ MB/s)
            // Configuration is read from quarkus.grpc.clients."*".* properties per Quarkus docs
            // Note: maxOutboundMessageSize is not available on NettyChannelBuilder, only on ManagedChannelBuilder
            return NettyChannelBuilder
                .forAddress(host, port)
                .initialFlowControlWindow(flowControlWindow)  // HTTP/2 initial window size
                .maxInboundMessageSize(maxInboundMessageSize)
                .usePlaintext()
                .build();
        });
        
        return createStub(stubClass, channel);
    }
    
    /**
     * Get a Mutiny stub client for a service by discovering it from Consul.
     * 
     * @param stubClass The Mutiny stub class
     * @param serviceName The Consul service name (e.g., "echo", "test")
     * @param <T> The stub type
     * @return A Uni that resolves to the Mutiny stub instance
     */
    @CacheResult(cacheName = CHANNEL_CACHE_NAME)
    public <T> Uni<T> getClientForService(Class<T> stubClass, String serviceName) {
        return serviceDiscovery.discoverService(serviceName)
            .map(instance -> {
                LOG.debug("Discovered service {} at {}:{}", 
                    serviceName, instance.getHost(), instance.getPort());
                return getClient(stubClass, instance.getHost(), instance.getPort());
            });
    }
    
    /**
     * Get a Mutiny stub client without caching the discovery result.
     * 
     * @param stubClass The Mutiny stub class
     * @param serviceName The Consul service name
     * @param <T> The stub type
     * @return A Uni that resolves to the Mutiny stub instance
     */
    public <T> Uni<T> getClientForServiceUncached(Class<T> stubClass, String serviceName) {
        return serviceDiscovery.discoverService(serviceName)
            .map(instance -> {
                LOG.debug("Discovered service {} at {}:{} (uncached)", 
                    serviceName, instance.getHost(), instance.getPort());
                return getClient(stubClass, instance.getHost(), instance.getPort());
            });
    }
    
    /**
     * Create a stub instance using reflection.
     * This works by finding the static newMutinyStub method on the parent class.
     */
    @SuppressWarnings("unchecked")
    private <T> T createStub(Class<T> stubClass, Channel channel) {
        try {
            // Get the enclosing class (e.g., MutinyPipeStepProcessorServiceGrpc)
            Class<?> grpcClass = stubClass.getEnclosingClass();
            if (grpcClass == null) {
                throw new IllegalArgumentException("Stub class must be a nested class of a gRPC service class");
            }
            
            // Find the newMutinyStub method
            Method newStubMethod = grpcClass.getMethod("newMutinyStub", Channel.class);
            
            // Create the stub
            return (T) newStubMethod.invoke(null, channel);
            
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to create Mutiny stub for " + stubClass.getName(), e);
        }
    }
    
    /**
     * Gracefully shuts down all managed channels.
     */
    @PreDestroy
    void shutdown() {
        LOG.info("Shutting down {} gRPC channels", channels.size());
        
        channels.forEach((key, channel) -> {
            try {
                LOG.debug("Shutting down channel for {}", key);
                channel.shutdown();
                
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Channel {} did not terminate gracefully, forcing shutdown", key);
                    channel.shutdownNow();
                }
            } catch (Exception e) {
                LOG.error("Error shutting down channel {}", key, e);
                try {
                    channel.shutdownNow();
                } catch (Exception ex) {
                    LOG.error("Error forcing shutdown of channel {}", key, ex);
                }
            }
        });
        
        channels.clear();
    }
    
    /**
     * Get the number of active channels.
     *
     * @return the number of ManagedChannel instances currently open
     */
    public int getActiveChannelCount() {
        return channels.size();
    }
    
    /**
     * Check if a channel exists for a specific host:port.
     *
     * @param host the target host
     * @param port the target port
     * @return true if a ManagedChannel for host:port is present in the cache
     */
    public boolean hasChannel(String host, int port) {
        return channels.containsKey(host + ":" + port);
    }
}