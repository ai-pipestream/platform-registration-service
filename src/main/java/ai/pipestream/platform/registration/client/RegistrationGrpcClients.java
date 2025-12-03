package ai.pipestream.platform.registration.client;

import ai.pipestream.quarkus.dynamicgrpc.GrpcClientFactory;
import ai.pipestream.data.module.v1.MutinyPipeStepProcessorServiceGrpc;
import ai.pipestream.platform.registration.v1.MutinyPlatformRegistrationServiceGrpc;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Service-specific gRPC client factory for platform-registration-service.
 * Provides type-safe methods using method references (zero reflection).
 */
@ApplicationScoped
public class RegistrationGrpcClients {

    @Inject
    GrpcClientFactory clientFactory;

    /**
     * Get a PipeStepProcessor client for a given service.
     *
     * @param serviceName The logical service name for discovery
     * @return A Uni emitting the PipeStepProcessor stub
     */
    public Uni<MutinyPipeStepProcessorServiceGrpc.MutinyPipeStepProcessorServiceStub> getPipeStepProcessorClient(String serviceName) {
        return clientFactory.getClient(
            serviceName,
            MutinyPipeStepProcessorServiceGrpc::newMutinyStub
        );
    }

    /**
     * Get a PlatformRegistration client for a given service.
     *
     * @param serviceName The logical service name for discovery
     * @return A Uni emitting the PlatformRegistration stub
     */
    public Uni<MutinyPlatformRegistrationServiceGrpc.MutinyPlatformRegistrationServiceStub> getPlatformRegistrationClient(String serviceName) {
        return clientFactory.getClient(
            serviceName,
            MutinyPlatformRegistrationServiceGrpc::newMutinyStub
        );
    }

    /**
     * Get the number of active service connections being managed.
     *
     * @return The count of active service channels
     */
    public int getActiveServiceCount() {
        return clientFactory.getActiveServiceCount();
    }

    /**
     * Evict (close) a cached channel for a service to force reconnection.
     *
     * @param serviceName The service name to evict
     */
    public void evictChannel(String serviceName) {
        clientFactory.evictChannel(serviceName);
    }

    /**
     * Get cache statistics for debugging and monitoring.
     *
     * @return String representation of cache statistics
     */
    public String getCacheStats() {
        return clientFactory.getCacheStats();
    }
}
