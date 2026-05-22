package ai.pipestream.platform.registration.client;

import ai.pipestream.quarkus.dynamicgrpc.GrpcClientFactory;
import ai.pipestream.platform.registration.v1.MutinyPlatformRegistrationServiceGrpc;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Service-specific gRPC client factory for platform-registration-service.
 * Module metadata is inline on {@code RegisterRequest} — no
 * {@code PipeStepProcessorService} callback clients.
 */
@ApplicationScoped
public class RegistrationGrpcClients {

    @Inject
    GrpcClientFactory clientFactory;

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

    public int getActiveServiceCount() {
        return clientFactory.getActiveServiceCount();
    }

    public void evictChannel(String serviceName) {
        clientFactory.evictChannel(serviceName);
    }

    public String getCacheStats() {
        return clientFactory.getCacheStats();
    }
}
