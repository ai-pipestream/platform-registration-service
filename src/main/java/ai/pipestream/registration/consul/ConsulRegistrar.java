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
import java.util.Map;

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
        
        // Use internal host/port for Consul service address (what Consul uses to reach the service)
        // Fall back to advertised if internal is not specified
        // This is critical for Docker environments where Consul needs to reach the service
        // via the Docker bridge network (e.g., 172.17.0.1) rather than host.docker.internal
        String consulHost = connectivity.hasInternalHost() 
                ? connectivity.getInternalHost() 
                : connectivity.getAdvertisedHost();
        int consulPort = connectivity.hasInternalPort() 
                ? connectivity.getInternalPort() 
                : connectivity.getAdvertisedPort();
        
        // Sanitize metadata keys (Consul doesn't allow dots in metadata keys)
        Map<String, String> sanitizedMetadata = sanitizeMetadataKeys(request.getMetadataMap());
        
        // Store advertised address in metadata for client discovery
        // Clients should use advertised-host/port, not the internal address
        sanitizedMetadata.put("advertised-host", connectivity.getAdvertisedHost());
        sanitizedMetadata.put("advertised-port", String.valueOf(connectivity.getAdvertisedPort()));
        
        // Add version to metadata
        sanitizedMetadata.put("version", request.getVersion());
        
        // Add service type to metadata for identification
        sanitizedMetadata.put("service-type", request.getType().name());

        // Add HTTP endpoints to metadata for discovery (if provided)
        if (request.getHttpEndpointsCount() > 0) {
            sanitizedMetadata.put("http_endpoint_count", String.valueOf(request.getHttpEndpointsCount()));
            for (int i = 0; i < request.getHttpEndpointsCount(); i++) {
                var endpoint = request.getHttpEndpoints(i);
                String prefix = "http_endpoint_" + i + "_";
                sanitizedMetadata.put(prefix + "scheme", endpoint.getScheme());
                sanitizedMetadata.put(prefix + "host", endpoint.getHost());
                sanitizedMetadata.put(prefix + "port", String.valueOf(endpoint.getPort()));
                if (!endpoint.getBasePath().isBlank()) {
                    sanitizedMetadata.put(prefix + "base_path", endpoint.getBasePath());
                }
                if (!endpoint.getHealthPath().isBlank()) {
                    sanitizedMetadata.put(prefix + "health_path", endpoint.getHealthPath());
                }
                sanitizedMetadata.put(prefix + "tls_enabled", String.valueOf(endpoint.getTlsEnabled()));
            }
        }

        if (request.hasHttpSchemaArtifactId()) {
            sanitizedMetadata.put("http_schema_artifact_id", request.getHttpSchemaArtifactId());
        }
        if (request.hasHttpSchemaVersion()) {
            sanitizedMetadata.put("http_schema_version", request.getHttpSchemaVersion());
        }
        
        ServiceOptions serviceOptions = new ServiceOptions()
                .setId(serviceId)
                .setName(request.getName())
                .setAddress(consulHost)  // Use internal host (reachable by Consul)
                .setPort(consulPort)     // Use internal port (reachable by Consul)
                .setTags(new ArrayList<>(request.getTagsList()))
                .setMeta(sanitizedMetadata);

        // Add capabilities as tags with prefix
        request.getCapabilitiesList().forEach(cap ->
                serviceOptions.getTags().add("capability:" + cap)
        );

        // Health checks use the same address as the service registration
        // (Consul will use the service's registered address for health checks)
        String healthCheckHost = consulHost;
        int healthCheckPort = consulPort;

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

    /**
     * Sanitize metadata keys by replacing dots with underscores.
     * Consul metadata keys cannot contain dots, so we replace them with underscores
     * (matching the pattern used elsewhere in the codebase, e.g., ApicurioRegistryClient).
     * 
     * @param metadata The original metadata map
     * @return A new map with sanitized keys
     */
    private Map<String, String> sanitizeMetadataKeys(Map<String, String> metadata) {
        Map<String, String> sanitized = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String sanitizedKey = entry.getKey().replace('.', '_');
            sanitized.put(sanitizedKey, entry.getValue());
        }
        return sanitized;
    }
}
