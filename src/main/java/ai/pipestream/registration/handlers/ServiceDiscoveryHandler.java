package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.platform.registration.v1.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.mutiny.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles service discovery and lookup operations.
 * Updated to use the new VerbNoun proto naming convention.
 */
@ApplicationScoped
public class ServiceDiscoveryHandler {

    private static final Logger LOG = Logger.getLogger(ServiceDiscoveryHandler.class);

    @Inject
    ConsulClient consulClient;

    /**
     * List all services (non-modules)
     */
    public Uni<ListServicesResponse> listServices() {
        return consulClient.catalogServices()
            .flatMap(services -> {
                if (services == null || services.getList() == null || services.getList().isEmpty()) {
                    return Uni.createFrom().item(buildEmptyServiceList());
                }

                // Get health info for each service
                List<Uni<List<GetServiceResponse>>> serviceUnis = services.getList().stream()
                    .map(service -> consulClient.healthServiceNodes(service.getName(), true)
                        .map(healthNodes -> {
                            if (healthNodes == null || healthNodes.getList() == null) {
                                return Collections.<GetServiceResponse>emptyList();
                            }
                            return healthNodes.getList().stream()
                                .filter(entry -> !isModule(entry.getService().getTags()))
                                .map(this::convertToGetServiceResponse)
                                .collect(Collectors.toList());
                        })
                        .onFailure().recoverWithItem(Collections.emptyList())
                    )
                    .collect(Collectors.toList());

                return Uni.join().all(serviceUnis).andCollectFailures()
                    .map(lists -> {
                        List<GetServiceResponse> allServices = lists.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());

                        return ListServicesResponse.newBuilder()
                            .addAllServices(allServices)
                            .setAsOf(createTimestamp())
                            .setTotalCount(allServices.size())
                            .build();
                    });
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.error("Failed to list services from Consul", throwable);
                return buildEmptyServiceList();
            });
    }

    /**
     * List all modules
     */
    public Uni<ListPlatformModulesResponse> listModules() {
        return consulClient.catalogServices()
            .flatMap(services -> {
                if (services == null || services.getList() == null || services.getList().isEmpty()) {
                    return Uni.createFrom().item(buildEmptyModuleList());
                }

                // Get health info for each service that is a module
                List<Uni<List<GetModuleResponse>>> moduleUnis = services.getList().stream()
                    .map(service -> consulClient.healthServiceNodes(service.getName(), true)
                        .map(healthNodes -> {
                            if (healthNodes == null || healthNodes.getList() == null) {
                                return Collections.<GetModuleResponse>emptyList();
                            }
                            return healthNodes.getList().stream()
                                .filter(entry -> isModule(entry.getService().getTags()))
                                .map(this::convertToGetModuleResponse)
                                .collect(Collectors.toList());
                        })
                        .onFailure().recoverWithItem(Collections.emptyList())
                    )
                    .collect(Collectors.toList());

                return Uni.join().all(moduleUnis).andCollectFailures()
                    .map(lists -> {
                        List<GetModuleResponse> allModules = lists.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());

                        return ListPlatformModulesResponse.newBuilder()
                            .addAllModules(allModules)
                            .setAsOf(createTimestamp())
                            .setTotalCount(allModules.size())
                            .build();
                    });
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.error("Failed to list modules from Consul", throwable);
                return buildEmptyModuleList();
            });
    }

    /**
     * Get service by name (returns first healthy instance)
     */
    public Uni<GetServiceResponse> getServiceByName(String serviceName) {
        return consulClient.healthServiceNodes(serviceName, true)
            .map(Unchecked.function(serviceEntries -> {
                if (serviceEntries == null || serviceEntries.getList() == null || serviceEntries.getList().isEmpty()) {
                    throw new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("Service not found: " + serviceName)
                    );
                }
                // Return first healthy instance
                return convertToGetServiceResponse(serviceEntries.getList().getFirst());
            }));
    }

    /**
     * Get service by ID
     */
    public Uni<GetServiceResponse> getServiceById(String serviceId) {
        // Consul doesn't have a direct "get by ID" so we need to extract service name and search
        String serviceName = extractServiceNameFromId(serviceId);
        if (serviceName == null) {
            return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(
                io.grpc.Status.INVALID_ARGUMENT.withDescription("Invalid service ID format: " + serviceId)
            ));
        }

        return consulClient.healthServiceNodes(serviceName, true)
            .map(serviceEntries -> {
                if (serviceEntries == null || serviceEntries.getList() == null) {
                    throw new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("Service not found: " + serviceId)
                    );
                }

                var entry = serviceEntries.getList().stream()
                    .filter(e -> serviceId.equals(e.getService().getId()))
                    .findFirst()
                    .orElseThrow(() -> new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("Service instance not found: " + serviceId)
                    ));

                return convertToGetServiceResponse(entry);
            });
    }

    /**
     * Get module by name
     */
    public Uni<GetModuleResponse> getModuleByName(String moduleName) {
        return consulClient.healthServiceNodes(moduleName, true)
            .map(serviceEntries -> {
                if (serviceEntries == null || serviceEntries.getList() == null || serviceEntries.getList().isEmpty()) {
                    throw new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("Module not found: " + moduleName)
                    );
                }
                // Return first healthy instance that is tagged as module
                var moduleEntry = serviceEntries.getList().stream()
                    .filter(entry -> isModule(entry.getService().getTags()))
                    .findFirst()
                    .orElseThrow(() -> new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("Module not found: " + moduleName)
                    ));

                return convertToGetModuleResponse(moduleEntry);
            });
    }

    /**
     * Get module by ID
     */
    public Uni<GetModuleResponse> getModuleById(String moduleId) {
        String moduleName = extractServiceNameFromId(moduleId);
        if (moduleName == null) {
            return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(
                io.grpc.Status.INVALID_ARGUMENT.withDescription("Invalid module ID format: " + moduleId)
            ));
        }

        return consulClient.healthServiceNodes(moduleName, true)
            .map(serviceEntries -> {
                if (serviceEntries == null || serviceEntries.getList() == null) {
                    throw new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("Module not found: " + moduleId)
                    );
                }

                var entry = serviceEntries.getList().stream()
                    .filter(e -> moduleId.equals(e.getService().getId()) && isModule(e.getService().getTags()))
                    .findFirst()
                    .orElseThrow(() -> new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("Module instance not found: " + moduleId)
                    ));

                return convertToGetModuleResponse(entry);
            });
    }

    /**
     * Resolve service to find the best available instance
     */
    public Uni<ResolveServiceResponse> resolveService(ResolveServiceRequest request) {
        String serviceName = request.getServiceName();

        return consulClient.healthServiceNodes(serviceName, true)
            .map(serviceEntries -> {
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

                List<io.vertx.ext.consul.ServiceEntry> healthyInstances = serviceEntries.getList();

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
                io.vertx.ext.consul.ServiceEntry selectedInstance = null;
                String selectionReason = "";

                if (request.getPreferLocal()) {
                    // Try to find an instance on localhost first
                    Optional<io.vertx.ext.consul.ServiceEntry> localInstance = healthyInstances.stream()
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
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to resolve service: %s", serviceName);
                return ResolveServiceResponse.newBuilder()
                    .setFound(false)
                    .setServiceName(serviceName)
                    .setSelectionReason("Error resolving service: " + throwable.getMessage())
                    .setResolvedAt(createTimestamp())
                    .build();
            });
    }

    private GetServiceResponse convertToGetServiceResponse(io.vertx.ext.consul.ServiceEntry entry) {
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

    private GetModuleResponse convertToGetModuleResponse(io.vertx.ext.consul.ServiceEntry entry) {
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
            String host = metadata.getOrDefault(prefix + "host", "");
            String portValue = metadata.getOrDefault(prefix + "port", "");
            if (host.isBlank() || portValue.isBlank()) {
                continue;
            }

            int port;
            try {
                port = Integer.parseInt(portValue);
            } catch (NumberFormatException e) {
                continue;
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

    /**
     * Watch for real-time updates to the list of all healthy services.
     * Sends an initial list immediately, then sends updates whenever services change.
     */
    public Multi<WatchServicesResponse> watchServices() {
        LOG.info("Starting service watch stream");

        // Send initial list immediately
        Multi<WatchServicesResponse> initialList = Multi.createFrom().uni(listServices())
            .map(this::convertToWatchServicesResponse)
            .onItem().invoke(response ->
                LOG.infof("Sending initial service list with %d services", response.getTotalCount())
            );

        // Then poll for changes every 2 seconds
        Multi<WatchServicesResponse> updates = Multi.createFrom().ticks().every(Duration.ofSeconds(2))
            .onItem().transformToUniAndConcatenate(tick -> listServices())
            .map(this::convertToWatchServicesResponse)
            .onItem().invoke(response ->
                LOG.debugf("Service watch update: %d services", response.getTotalCount())
            )
            .onFailure().invoke(throwable ->
                LOG.error("Error during service watch", throwable)
            )
            .onFailure().recoverWithItem(throwable -> {
                LOG.error("Recovering from error in service watch", throwable);
                return buildEmptyWatchServicesResponse();
            });

        // Combine initial list with ongoing updates
        return Multi.createBy().concatenating()
            .streams(initialList, updates)
            .onCompletion().invoke(() -> LOG.info("Service watch stream completed"))
            .onCancellation().invoke(() -> LOG.info("Service watch stream cancelled by client"));
    }

    /**
     * Watch for real-time updates to the list of all registered modules.
     * Sends an initial list immediately, then sends updates whenever modules change.
     */
    public Multi<WatchModulesResponse> watchModules() {
        LOG.info("Starting module watch stream");

        // Send initial list immediately
        Multi<WatchModulesResponse> initialList = Multi.createFrom().uni(listModules())
            .map(this::convertToWatchModulesResponse)
            .onItem().invoke(response ->
                LOG.infof("Sending initial module list with %d modules", response.getTotalCount())
            );

        // Then poll for changes every 2 seconds
        Multi<WatchModulesResponse> updates = Multi.createFrom().ticks().every(Duration.ofSeconds(2))
            .onItem().transformToUniAndConcatenate(tick -> listModules())
            .map(this::convertToWatchModulesResponse)
            .onItem().invoke(response ->
                LOG.debugf("Module watch update: %d modules", response.getTotalCount())
            )
            .onFailure().invoke(throwable ->
                LOG.error("Error during module watch", throwable)
            )
            .onFailure().recoverWithItem(throwable -> {
                LOG.error("Recovering from error in module watch", throwable);
                return buildEmptyWatchModulesResponse();
            });

        // Combine initial list with ongoing updates
        return Multi.createBy().concatenating()
            .streams(initialList, updates)
            .onCompletion().invoke(() -> LOG.info("Module watch stream completed"))
            .onCancellation().invoke(() -> LOG.info("Module watch stream cancelled by client"));
    }

    private WatchServicesResponse convertToWatchServicesResponse(ListServicesResponse listResponse) {
        return WatchServicesResponse.newBuilder()
            .addAllServices(listResponse.getServicesList())
            .setAsOf(listResponse.getAsOf())
            .setTotalCount(listResponse.getTotalCount())
            .build();
    }

    private WatchModulesResponse convertToWatchModulesResponse(ListPlatformModulesResponse listResponse) {
        return WatchModulesResponse.newBuilder()
            .addAllModules(listResponse.getModulesList())
            .setAsOf(listResponse.getAsOf())
            .setTotalCount(listResponse.getTotalCount())
            .build();
    }

    private WatchServicesResponse buildEmptyWatchServicesResponse() {
        return WatchServicesResponse.newBuilder()
            .setAsOf(createTimestamp())
            .setTotalCount(0)
            .build();
    }

    private WatchModulesResponse buildEmptyWatchModulesResponse() {
        return WatchModulesResponse.newBuilder()
            .setAsOf(createTimestamp())
            .setTotalCount(0)
            .build();
    }
}
