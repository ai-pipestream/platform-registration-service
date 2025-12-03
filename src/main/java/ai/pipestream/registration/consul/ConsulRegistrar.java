package ai.pipestream.registration.consul;

import ai.pipestream.platform.registration.v1.Connectivity;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.consul.CheckOptions;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.mutiny.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Handles service registration and unregistration with Consul.
 * Updated to use the unified RegisterRequest from the new proto API.
 */
@ApplicationScoped
public class ConsulRegistrar {

    private static final Logger LOG = Logger.getLogger(ConsulRegistrar.class);

    @Inject
    ConsulClient consulClient;

    /**
     * Register a service with Consul including health check configuration.
     * 
     * @param request The unified RegisterRequest (works for both services and modules)
     * @param serviceId The unique Consul service ID to use
     * @return A Uni indicating success or failure
     */
    public Uni<Boolean> registerService(RegisterRequest request, String serviceId) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        
        ServiceOptions serviceOptions = new ServiceOptions()
                .setId(serviceId)
                .setName(request.getName())
                .setAddress(host)
                .setPort(port)
                .setTags(new ArrayList<>(request.getTagsList()))
                .setMeta(new HashMap<>(request.getMetadataMap()));

        // Add version to metadata
        serviceOptions.getMeta().put("version", request.getVersion());
        
        // Add service type to metadata for identification
        serviceOptions.getMeta().put("service-type", request.getType().name());

        // Add capabilities as tags with prefix
        request.getCapabilitiesList().forEach(cap ->
                serviceOptions.getTags().add("capability:" + cap)
        );

        // Determine the host for health checks (prefer internal if available)
        String healthCheckHost = connectivity.hasInternalHost() 
                ? connectivity.getInternalHost() 
                : host;
        int healthCheckPort = connectivity.hasInternalPort() 
                ? connectivity.getInternalPort() 
                : port;

        // Configure gRPC health check
        CheckOptions checkOptions = new CheckOptions()
                .setName(request.getName() + " gRPC Health Check")
                .setGrpc(healthCheckHost + ":" + healthCheckPort)
                .setInterval("10s")
                .setDeregisterAfter("1m");

        serviceOptions.setCheckOptions(checkOptions);

        LOG.infof("Registering service with Consul: %s (type=%s)", serviceId, request.getType());

        return consulClient.registerService(serviceOptions)
                .map(v -> {
                    LOG.infof("Successfully registered service: %s", serviceId);
                    return true;
                })
                .onFailure().recoverWithItem(throwable -> {
                    LOG.errorf(throwable, "Failed to register service: %s", serviceId);
                    return false;
                });
    }

    /**
     * Unregister a service from Consul
     */
    public Uni<Boolean> unregisterService(String serviceId) {
        LOG.infof("Unregistering service from Consul: %s", serviceId);

        return consulClient.deregisterService(serviceId)
                .map(v -> {
                    LOG.infof("Successfully unregistered service: %s", serviceId);
                    return true;
                })
                .onFailure().recoverWithItem(throwable -> {
                    LOG.errorf(throwable, "Failed to unregister service: %s", serviceId);
                    return false;
                });
    }

    /**
     * Generate a consistent service ID from service details
     */
    public static String generateServiceId(String serviceName, String host, int port) {
        return String.format("%s-%s-%d", serviceName, host, port);
    }
}
