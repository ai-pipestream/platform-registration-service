package ai.pipestream.dynamic.grpc.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.quarkus.grpc.runtime.stork.StorkGrpcChannel;
import io.quarkus.grpc.runtime.config.GrpcClientConfiguration;
import io.vertx.grpc.client.GrpcClient;
import io.vertx.grpc.client.GrpcClientOptions;
import io.vertx.core.Vertx;
import java.util.concurrent.Executor;
import io.smallrye.mutiny.Uni;
import io.smallrye.stork.api.ServiceInstance;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages gRPC Channels for services.
 * <p>
 * Channels are cached with Caffeine and automatically evicted after an idle TTL.
 * On eviction or application shutdown, channels are shut down gracefully.
 */
@ApplicationScoped
public class ChannelManager {

    /**
     * Default constructor for CDI frameworks.
     */
    public ChannelManager() { }

    private static final Logger LOG = Logger.getLogger(ChannelManager.class);
    
    @jakarta.inject.Inject
    Vertx vertx;
    
    @jakarta.inject.Inject
    Executor executor;

    // Configuration with sensible defaults
    @ConfigProperty(name = "pipeline.grpc.channel.idle-ttl-minutes", defaultValue = "15")
    long channelIdleTtlMinutes;
    
    @ConfigProperty(name = "pipeline.grpc.channel.max-size", defaultValue = "1000")
    long channelMaxSize;
    
    @ConfigProperty(name = "pipeline.grpc.channel.shutdown-timeout-seconds", defaultValue = "2")
    long shutdownTimeoutSeconds;

    // Self-cleaning cache for channels with automatic eviction and shutdown
    private Cache<String, Channel> channelCache;
    // Flag to avoid creating new channels during application shutdown
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    @jakarta.annotation.PostConstruct
    void init() {
        this.channelCache = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(channelIdleTtlMinutes))
            .maximumSize(channelMaxSize)
            .removalListener(this::onChannelRemoved)
            .recordStats() // Enable stats for monitoring
            .build();
        
        LOG.infof("Initialized ChannelManager with TTL=%d minutes, max size=%d", 
                  channelIdleTtlMinutes, channelMaxSize);
    }

    // Channel removal listener - ensures proper cleanup
    private void onChannelRemoved(String serviceName, Channel channel, RemovalCause cause) {
        if (channel == null) return;
        
        // Don't block on shutdown if we're already shutting down the application
        if (shuttingDown.get()) {
            LOG.debugf("Application shutting down, initiating non-blocking channel shutdown for service '%s'", serviceName);
            try {
                if (channel instanceof ManagedChannel) {
                    ((ManagedChannel) channel).shutdownNow();
                } else if (channel instanceof StorkGrpcChannel) {
                    ((StorkGrpcChannel) channel).close();
                }
            } catch (Exception e) {
                LOG.debugf("Error during shutdown of channel for service %s: %s", serviceName, e.getMessage());
            }
            return;
        }
        
        LOG.infof("Evicting gRPC channel for service '%s' due to: %s", serviceName, cause);
        
        try {
            if (channel instanceof ManagedChannel) {
                ManagedChannel mc = (ManagedChannel) channel;
                mc.shutdown();
                // Try graceful shutdown first with shorter timeout
                if (!mc.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    LOG.warnf("Channel for service %s did not terminate gracefully, forcing shutdown", serviceName);
                    mc.shutdownNow();
                }
            } else if (channel instanceof StorkGrpcChannel) {
                ((StorkGrpcChannel) channel).close();
            }
            LOG.debugf("Successfully shut down channel for service: %s", serviceName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.errorf("Interrupted while shutting down channel for service %s", serviceName);
            try {
                if (channel instanceof ManagedChannel) {
                    ((ManagedChannel) channel).shutdownNow();
                }
            } catch (Exception ex) {
                LOG.errorf(ex, "Error forcing shutdown of channel for service %s", serviceName);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Error shutting down channel for service %s", serviceName);
            try {
                if (channel instanceof ManagedChannel) {
                    ((ManagedChannel) channel).shutdownNow();
                }
            } catch (Exception ex) {
                LOG.errorf(ex, "Error forcing shutdown of channel for service %s", serviceName);
            }
        }
    }

    /**
     * Gets or creates a gRPC Channel for the given service.
     * Reuses a cached channel when available, otherwise creates a new Stork-backed channel.
     *
     * @param serviceName the logical service name used for discovery and caching
     * @param instances the list of discovered service instances (must be non-empty)
     * @return a Uni that emits the Channel when ready or fails with Status.UNAVAILABLE when no instances are provided
     */
    public Uni<Channel> getOrCreateChannel(String serviceName, List<ServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(
                    io.grpc.Status.UNAVAILABLE.withDescription("No instances found for service " + serviceName)
            ));
        }

        // Avoid creating/using channels during shutdown
        if (shuttingDown.get()) {
            return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(
                    io.grpc.Status.UNAVAILABLE.withDescription("Channel manager is shutting down")));
        }

        // Initialize cache if not already done (for testing scenarios)
        if (channelCache == null) {
            init();
        }

        // reuse existing healthy channel if present
        Channel existing = channelCache.getIfPresent(serviceName);
        if (existing != null) {
            LOG.debugf("Reusing existing gRPC channel for service: %s (type=%s)", serviceName, existing.getClass().getName());
            return Uni.createFrom().item(existing);
        }
        
        LOG.infof("Creating new Stork gRPC channel for service: %s", serviceName);
        
        // Create GrpcClient (same way as Quarkus does it)
        GrpcClientOptions clientOptions = new GrpcClientOptions();
        GrpcClient grpcClient = GrpcClient.client(vertx, clientOptions);
        
        // Create default StorkConfig
        GrpcClientConfiguration.StorkConfig storkConfig = new GrpcClientConfiguration.StorkConfig() {
            @Override
            public int threads() { return 10; }
            @Override
            public long deadline() { return 5000; }
            @Override
            public int retries() { return 3; }
            @Override
            public long delay() { return 60; }
            @Override
            public long period() { return 120; }
        };
        
        Channel created = new StorkGrpcChannel(grpcClient, serviceName, storkConfig, executor);
        LOG.debugf("Created StorkGrpcChannel for %s: type=%s", serviceName, created.getClass().getName());
        channelCache.put(serviceName, created);
        return Uni.createFrom().item(created);
    }

    /**
     * Manually evicts a channel for a service from the cache.
     *
     * @param serviceName the service whose channel should be removed
     */
    public void evictChannel(String serviceName) {
        LOG.infof("Manually evicting channel for service: %s", serviceName);
        channelCache.invalidate(serviceName);
    }

    /**
     * Gets cache statistics for monitoring.
     *
     * @return a human-readable summary of cache size, hits, misses, hit rate, and evictions
     */
    public String getCacheStats() {
        var stats = channelCache.stats();
        return String.format("Cache stats - Size: %d, Hits: %d, Misses: %d, Hit rate: %.2f%%, Evictions: %d",
            channelCache.estimatedSize(),
            stats.hitCount(),
            stats.missCount(), 
            stats.hitRate() * 100,
            stats.evictionCount());
    }

    /**
     * Gets the number of active services with cached channels.
     *
     * @return approximate count of active services (based on cache size)
     */
    public int getActiveServiceCount() {
        // Note: this is approximate due to Caffeine's internal state management
        return Math.toIntExact(channelCache.estimatedSize());
    }

    @PreDestroy
    void cleanup() {
        // Prevent new channels from being created during shutdown
        shuttingDown.set(true);

        if (channelCache == null) {
            LOG.debug("No channel cache to clean up");
            return;
        }
        
        LOG.infof("Shutting down %d cached gRPC channels on application exit...", channelCache.estimatedSize());
        
        // Create a snapshot of all channels before invalidation
        var channels = new java.util.ArrayList<>(channelCache.asMap().values());
        
        // Trigger eviction and removal listener for all entries
        channelCache.invalidateAll();
        
        // Process any pending maintenance operations
        channelCache.cleanUp();
        
        // Use ExecutorService to enforce overall timeout
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                // Shutdown all channels with force after timeout
                for (Channel channel : channels) {
                    try {
                        if (channel instanceof ManagedChannel) {
                            ManagedChannel mc = (ManagedChannel) channel;
                            if (!mc.isShutdown()) {
                                mc.shutdown();
                                if (!mc.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                                    mc.shutdownNow();
                                }
                            }
                        } else if (channel instanceof StorkGrpcChannel) {
                            ((StorkGrpcChannel) channel).close();
                        }
                    } catch (Exception e) {
                        LOG.debugf("Error during channel shutdown: %s", e.getMessage());
                        try {
                            if (channel instanceof ManagedChannel) {
                                ((ManagedChannel) channel).shutdownNow();
                            }
                        } catch (Exception ex) {
                            // Ignore - we're shutting down anyway
                        }
                    }
                }
            }).get(shutdownTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.warn("Channel shutdown timed out, forcing immediate termination");
            // Force shutdown any remaining channels
            channels.forEach(ch -> {
                try {
                    if (ch instanceof ManagedChannel) {
                        ManagedChannel mc = (ManagedChannel) ch;
                        if (!mc.isShutdown()) mc.shutdownNow();
                    } else if (ch instanceof StorkGrpcChannel) {
                        ((StorkGrpcChannel) ch).close();
                    }
                } catch (Exception ex) {
                    // Ignore
                }
            });
        } catch (Exception e) {
            LOG.error("Error during channel cleanup", e);
        } finally {
            executor.shutdownNow();
        }
        
        LOG.info("ChannelManager cleanup complete.");
    }
}
