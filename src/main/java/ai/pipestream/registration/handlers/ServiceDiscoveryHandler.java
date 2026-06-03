package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.platform.registration.v1.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.vertx.core.Future;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ServiceEntry;
import io.vertx.ext.consul.ServiceEntryList;
import io.vertx.ext.consul.ServiceList;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Handles service discovery and lookup operations.
 *
 * <p>Uses the standard Vert.x {@link ConsulClient} (async-only); each call is bridged to
 * blocking via {@code Future#toCompletionStage()}. Callers run on virtual threads, so this
 * never pins the event loop.
 */
@ApplicationScoped
public class ServiceDiscoveryHandler {

    private static final Logger LOG = Logger.getLogger(ServiceDiscoveryHandler.class);

    @Inject
    ConsulClient consulClient;

    /** Bridge a Vert.x async result to a blocking call on the current (virtual) thread. */
    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause != null ? cause : e);
        }
    }

    /**
     * List all services (non-modules).
     */
    public ListServicesResponse listServices() {
        try {
            ServiceList services = await(consulClient.catalogServices());
            if (services == null || services.getList() == null || services.getList().isEmpty()) {
                return buildEmptyServiceList();
            }

            List<GetServiceResponse> allServices = new ArrayList<>();
            for (var service : services.getList()) {
                try {
                    ServiceEntryList healthNodes =
                            await(consulClient.healthServiceNodes(service.getName(), true));
                    if (healthNodes != null && healthNodes.getList() != null) {
                        healthNodes.getList().stream()
                                .filter(entry -> !isModule(entry.getService().getTags()))
                                .map(this::convertToGetServiceResponse)
                                .forEach(allServices::add);
                    }
                } catch (Exception e) {
                    // Per-service failure is isolated — skip this service.
                    LOG.debugf(e, "Failed to read health nodes for service %s", service.getName());
                }
            }

            return ListServicesResponse.newBuilder()
                    .addAllServices(allServices)
                    .setAsOf(createTimestamp())
                    .setTotalCount(allServices.size())
                    .build();
        } catch (Exception throwable) {
            LOG.error("Failed to list services from Consul", throwable);
            return buildEmptyServiceList();
        }
    }

    /**
     * List all modules.
     */
    public ListPlatformModulesResponse listModules() {
        try {
            ServiceList services = await(consulClient.catalogServices());
            if (services == null || services.getList() == null || services.getList().isEmpty()) {
                return buildEmptyModuleList();
            }

            List<GetModuleResponse> allModules = new ArrayList<>();
            for (var service : services.getList()) {
                try {
                    ServiceEntryList healthNodes =
                            await(consulClient.healthServiceNodes(service.getName(), true));
                    if (healthNodes != null && healthNodes.getList() != null) {
                        healthNodes.getList().stream()
                                .filter(entry -> isModule(entry.getService().getTags()))
                                .map(this::convertToGetModuleResponse)
                                .forEach(allModules::add);
                    }
                } catch (Exception e) {
                    // Per-service failure is isolated — skip this service.
                    LOG.debugf(e, "Failed to read health nodes for service %s", service.getName());
                }
            }

            return ListPlatformModulesResponse.newBuilder()
                    .addAllModules(allModules)
                    .setAsOf(createTimestamp())
                    .setTotalCount(allModules.size())
                    .build();
        } catch (Exception throwable) {
            LOG.error("Failed to list modules from Consul", throwable);
            return buildEmptyModuleList();
        }
    }

    /**
     * Get service by name (returns first healthy instance).
     */
    public GetServiceResponse getServiceByName(String serviceName) {
        ServiceEntryList serviceEntries =
                await(consulClient.healthServiceNodes(serviceName, true));
        if (serviceEntries == null || serviceEntries.getList() == null || serviceEntries.getList().isEmpty()) {
            throw new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("Service not found: " + serviceName)
            );
        }
        // Return first healthy instance
        return convertToGetServiceResponse(serviceEntries.getList().getFirst());
    }

    /**
     * Get service by ID.
     */
    public GetServiceResponse getServiceById(String serviceId) {
        // Consul doesn't have a direct "get by ID" so we need to extract service name and search
        String serviceName = extractServiceNameFromId(serviceId);
        if (serviceName == null) {
            throw new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("Invalid service ID format: " + serviceId)
            );
        }

        ServiceEntryList serviceEntries =
                await(consulClient.healthServiceNodes(serviceName, true));
        if (serviceEntries == null || serviceEntries.getList() == null) {
            throw new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("Service not found: " + serviceId)
            );
        }

        ServiceEntry entry = serviceEntries.getList().stream()
                .filter(e -> serviceId.equals(e.getService().getId()))
                .findFirst()
                .orElseThrow(() -> new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("Service instance not found: " + serviceId)
                ));

        return convertToGetServiceResponse(entry);
    }

    /**
     * Get module by name.
     */
    public GetModuleResponse getModuleByName(String moduleName) {
        ServiceEntryList serviceEntries =
                await(consulClient.healthServiceNodes(moduleName, true));
        if (serviceEntries == null || serviceEntries.getList() == null || serviceEntries.getList().isEmpty()) {
            throw new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("Module not found: " + moduleName)
            );
        }
        // Return first healthy instance that is tagged as module
        ServiceEntry moduleEntry = serviceEntries.getList().stream()
                .filter(entry -> isModule(entry.getService().getTags()))
                .findFirst()
                .orElseThrow(() -> new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("Module not found: " + moduleName)
                ));

        return convertToGetModuleResponse(moduleEntry);
    }

    /**
     * Get module by ID.
     */
    public GetModuleResponse getModuleById(String moduleId) {
        String moduleName = extractServiceNameFromId(moduleId);
        if (moduleName == null) {
            throw new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("Invalid module ID format: " + moduleId)
            );
        }

        ServiceEntryList serviceEntries =
                await(consulClient.healthServiceNodes(moduleName, true));
        if (serviceEntries == null || serviceEntries.getList() == null) {
            throw new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("Module not found: " + moduleId)
            );
        }

        ServiceEntry entry = serviceEntries.getList().stream()
                .filter(e -> moduleId.equals(e.getService().getId()) && isModule(e.getService().getTags()))
                .findFirst()
                .orElseThrow(() -> new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("Module instance not found: " + moduleId)
                ));

        return convertToGetModuleResponse(entry);
    }

    /**
     * Resolve service to find the best available instance.
     */
    public ResolveServiceResponse resolveService(ResolveServiceRequest request) {
        String serviceName = request.getServiceName();

        try {
            ServiceEntryList serviceEntries =
                    await(consulClient.healthServiceNodes(serviceName, true));

            ResolveServiceResponse.Builder responseBuilder = ResolveServiceResponse.newBuilder()
                    .setServiceName(serviceName)
                    .setResolvedAt(createTimestamp());

            if (serviceEntries == null || serviceEntries.getList() == null || serviceEntries.getList().isEmpty()) {
                // No healthy instances found
                return responseBuilder
                        .setFound(false)
                        .setTotalInstances(0)
                        .setHealthyInstances(0)
                        .setSelectionReason("No healthy instances found")
                        .build();
            }

            List<ServiceEntry> healthyInstances = serviceEntries.getList();

            // Filter by required tags if specified
            if (!request.getRequiredTagsList().isEmpty()) {
                healthyInstances = healthyInstances.stream()
                        .filter(entry -> {
                            List<String> serviceTags = entry.getService().getTags();
                            if (serviceTags == null) return false;
                            return new HashSet<>(serviceTags).containsAll(request.getRequiredTagsList());
                        })
                        .collect(Collectors.toList());
            }

            // Filter by required capabilities if specified
            if (!request.getRequiredCapabilitiesList().isEmpty()) {
                healthyInstances = healthyInstances.stream()
                        .filter(entry -> {
                            List<String> serviceTags = entry.getService().getTags();
                            if (serviceTags == null) return false;
                            // Capabilities are stored as "capability:xxx" tags
                            Set<String> capabilities = serviceTags.stream()
                                    .filter(tag -> tag.startsWith("capability:"))
                                    .map(tag -> tag.substring("capability:".length()))
                                    .collect(Collectors.toSet());
                            return capabilities.containsAll(request.getRequiredCapabilitiesList());
                        })
                        .collect(Collectors.toList());
            }

            if (healthyInstances.isEmpty()) {
                // No instances match the criteria
                return responseBuilder
                        .setFound(false)
                        .setTotalInstances(serviceEntries.getList().size())
                        .setHealthyInstances(serviceEntries.getList().size())
                        .setSelectionReason("No instances match the required criteria")
                        .build();
            }

            // Select the best instance
            ServiceEntry selectedInstance = null;
            String selectionReason = "";

            if (request.getPreferLocal()) {
                // Try to find an instance on localhost first
                Optional<ServiceEntry> localInstance = healthyInstances.stream()
                        .filter(entry -> "localhost".equals(entry.getService().getAddress()) ||
                                        "127.0.0.1".equals(entry.getService().getAddress()))
                        .findFirst();

                if (localInstance.isPresent()) {
                    selectedInstance = localInstance.get();
                    selectionReason = "Selected local instance as requested";
                }
            }

            if (selectedInstance == null) {
                // Use round-robin or random selection
                // For now, just pick the first one (could be enhanced with load balancing)
                selectedInstance = healthyInstances.getFirst();
                selectionReason = "Selected first available healthy instance";
            }

            var service = selectedInstance.getService();
            responseBuilder
                    .setFound(true)
                    .setHost(service.getAddress())
                    .setPort(service.getPort())
                    .setServiceId(service.getId())
                    .setTotalInstances(serviceEntries.getList().size())
                    .setHealthyInstances(healthyInstances.size())
                    .setSelectionReason(selectionReason);

            // Add metadata
            if (service.getMeta() != null) {
                responseBuilder.putAllMetadata(service.getMeta());
                if (service.getMeta().containsKey("version")) {
                    responseBuilder.setVersion(service.getMeta().get("version"));
                }
                responseBuilder.addAllHttpEndpoints(parseHttpEndpoints(service.getMeta()));
                if (service.getMeta().containsKey("http_schema_artifact_id")) {
                    responseBuilder.setHttpSchemaArtifactId(service.getMeta().get("http_schema_artifact_id"));
                }
                if (service.getMeta().containsKey("http_schema_version")) {
                    responseBuilder.setHttpSchemaVersion(service.getMeta().get("http_schema_version"));
                }
            }

            // Add tags and capabilities
            if (service.getTags() != null) {
                for (String tag : service.getTags()) {
                    if (tag.startsWith("capability:")) {
                        responseBuilder.addCapabilities(tag.substring("capability:".length()));
                    } else {
                        responseBuilder.addTags(tag);
                    }
                }
            }

            return responseBuilder.build();
        } catch (Exception throwable) {
            LOG.errorf(throwable, "Failed to resolve service: %s", serviceName);
            return ResolveServiceResponse.newBuilder()
                    .setFound(false)
                    .setServiceName(serviceName)
                    .setSelectionReason("Error resolving service: " + throwable.getMessage())
                    .setResolvedAt(createTimestamp())
                    .build();
        }
    }

    private GetServiceResponse convertToGetServiceResponse(ServiceEntry entry) {
        var service = entry.getService();
        GetServiceResponse.Builder builder = GetServiceResponse.newBuilder()
            .setServiceId(service.getId())
            .setServiceName(service.getName())
            .setHost(service.getAddress())
            .setPort(service.getPort())
            .setIsHealthy(true); // Only healthy services are returned by default

        // Extract metadata
        if (service.getMeta() != null) {
            builder.putAllMetadata(service.getMeta());
            if (service.getMeta().containsKey("version")) {
                builder.setVersion(service.getMeta().get("version"));
            }
            builder.addAllHttpEndpoints(parseHttpEndpoints(service.getMeta()));
            if (service.getMeta().containsKey("http_schema_artifact_id")) {
                builder.setHttpSchemaArtifactId(service.getMeta().get("http_schema_artifact_id"));
            }
            if (service.getMeta().containsKey("http_schema_version")) {
                builder.setHttpSchemaVersion(service.getMeta().get("http_schema_version"));
            }
        }

        // Extract tags and capabilities
        if (service.getTags() != null) {
            for (String tag : service.getTags()) {
                if (tag.startsWith("capability:")) {
                    builder.addCapabilities(tag.substring("capability:".length()));
                } else {
                    builder.addTags(tag);
                }
            }
        }

        builder.setRegisteredAt(createTimestamp());
        builder.setLastHealthCheck(createTimestamp());

        return builder.build();
    }

    private GetModuleResponse convertToGetModuleResponse(ServiceEntry entry) {
        var service = entry.getService();
        GetModuleResponse.Builder builder = GetModuleResponse.newBuilder()
            .setServiceId(service.getId())
            .setModuleName(service.getName())
            .setHost(service.getAddress())
            .setPort(service.getPort())
            .setIsHealthy(true);

        // Extract metadata
        if (service.getMeta() != null) {
            builder.putAllMetadata(service.getMeta());
            if (service.getMeta().containsKey("version")) {
                builder.setVersion(service.getMeta().get("version"));
            }
            // Extract module-specific fields from metadata
            if (service.getMeta().containsKey("input-format")) {
                builder.setInputFormat(service.getMeta().get("input-format"));
            }
            if (service.getMeta().containsKey("output-format")) {
                builder.setOutputFormat(service.getMeta().get("output-format"));
            }
        }

        builder.setRegisteredAt(createTimestamp());
        builder.setLastHealthCheck(createTimestamp());

        return builder.build();
    }

    /* package-private for testing */ List<HttpEndpoint> parseHttpEndpoints(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyList();
        }

        String countValue = metadata.get("http_endpoint_count");
        if (countValue == null || countValue.isBlank()) {
            return Collections.emptyList();
        }

        int count;
        try {
            count = Integer.parseInt(countValue);
        } catch (NumberFormatException e) {
            LOG.debugf("Invalid http_endpoint_count metadata: %s", countValue);
            return Collections.emptyList();
        }

        List<HttpEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String prefix = "http_endpoint_" + i + "_";
            String scheme = metadata.getOrDefault(prefix + "scheme", "");

            // Skip if no scheme is provided - we need at least that to know it's an HTTP endpoint
            if (scheme.isBlank()) {
                continue;
            }

            String host = metadata.getOrDefault(prefix + "host", "");
            String portValue = metadata.getOrDefault(prefix + "port", "");

            int port = 0;
            if (!portValue.isBlank()) {
                try {
                    port = Integer.parseInt(portValue);
                } catch (NumberFormatException e) {
                    LOG.debugf("Invalid port value for endpoint %d: %s, defaulting to 0", i, portValue);
                    // port stays 0
                }
            }

            HttpEndpoint.Builder endpointBuilder = HttpEndpoint.newBuilder()
                .setScheme(scheme)
                .setHost(host)
                .setPort(port);

            String basePath = metadata.getOrDefault(prefix + "base_path", "");
            if (!basePath.isBlank()) {
                endpointBuilder.setBasePath(basePath);
            }

            String healthPath = metadata.getOrDefault(prefix + "health_path", "");
            if (!healthPath.isBlank()) {
                endpointBuilder.setHealthPath(healthPath);
            }

            String tlsValue = metadata.getOrDefault(prefix + "tls_enabled", "");
            if (!tlsValue.isBlank()) {
                endpointBuilder.setTlsEnabled(Boolean.parseBoolean(tlsValue));
            }

            endpoints.add(endpointBuilder.build());
        }

        return endpoints;
    }

    private boolean isModule(List<String> tags) {
        return tags != null && tags.contains("module");
    }

    private String extractServiceNameFromId(String serviceId) {
        // Format: serviceName-host-port
        int lastDash = serviceId.lastIndexOf('-');
        if (lastDash == -1) return null;

        String withoutPort = serviceId.substring(0, lastDash);
        int secondLastDash = withoutPort.lastIndexOf('-');
        if (secondLastDash == -1) return null;

        return withoutPort.substring(0, secondLastDash);
    }

    private ListServicesResponse buildEmptyServiceList() {
        return ListServicesResponse.newBuilder()
            .setAsOf(createTimestamp())
            .setTotalCount(0)
            .build();
    }

    private ListPlatformModulesResponse buildEmptyModuleList() {
        return ListPlatformModulesResponse.newBuilder()
            .setAsOf(createTimestamp())
            .setTotalCount(0)
            .build();
    }

    private Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }
}
