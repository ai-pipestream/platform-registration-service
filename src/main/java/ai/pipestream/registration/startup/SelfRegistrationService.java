package ai.pipestream.registration.startup;

import ai.pipestream.platform.registration.v1.Connectivity;
import ai.pipestream.platform.registration.v1.HttpEndpoint;
import ai.pipestream.platform.registration.v1.PlatformEventType;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.ServiceType;
import ai.pipestream.registration.consul.ConsulRegistrar;
import ai.pipestream.registration.handlers.ServiceRegistrationHandler;
import io.quarkus.grpc.runtime.GrpcServerRecorder;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles self-registration of the platform-registration-service with Consul.
 * This bypasses the gRPC client (which would create a circular dependency) and
 * calls the local ServiceRegistrationHandler directly.
 */
@ApplicationScoped
public class SelfRegistrationService {

    private static final Logger LOG = Logger.getLogger(SelfRegistrationService.class);
    private static final Set<String> RESERVED_GRPC_SERVICES = Set.of(
            "grpc.health.v1.Health",
            "grpc.reflection.v1.ServerReflection",
            "grpc.reflection.v1alpha.ServerReflection"
    );

    @Inject
    ServiceRegistrationHandler serviceRegistrationHandler;

    @Inject
    ConsulClient consulClient;

    @ConfigProperty(name = "pipestream.registration.enabled", defaultValue = "false")
    boolean registrationEnabled;

    @ConfigProperty(name = "pipestream.registration.service-name", defaultValue = "")
    String serviceName;

    @ConfigProperty(name = "pipestream.registration.description", defaultValue = "")
    String description;

    @ConfigProperty(name = "pipestream.registration.type", defaultValue = "SERVICE")
    String serviceType;

    @ConfigProperty(name = "pipestream.registration.advertised-host", defaultValue = "localhost")
    String serviceHost;

    @ConfigProperty(name = "pipestream.registration.advertised-port", defaultValue = "0")
    int servicePort;

    @ConfigProperty(name = "pipestream.registration.internal-host", defaultValue = "")
    String internalHost;

    @ConfigProperty(name = "pipestream.registration.internal-port", defaultValue = "0")
    int internalPort;

    @ConfigProperty(name = "pipestream.registration.capabilities", defaultValue = "")
    String capabilities;

    @ConfigProperty(name = "pipestream.registration.tags", defaultValue = "")
    String tags;

    @ConfigProperty(name = "pipestream.registration.http.enabled", defaultValue = "true")
    boolean httpRegistrationEnabled;

    @ConfigProperty(name = "pipestream.registration.http.scheme", defaultValue = "http")
    String httpScheme;

    @ConfigProperty(name = "pipestream.registration.http.advertised-host")
    Optional<String> httpAdvertisedHost;

    @ConfigProperty(name = "pipestream.registration.http.advertised-port")
    Optional<Integer> httpAdvertisedPort;

    @ConfigProperty(name = "pipestream.registration.http.base-path")
    Optional<String> httpBasePath;

    @ConfigProperty(name = "pipestream.registration.http.health-path", defaultValue = "/q/health")
    String httpHealthPath;

    @ConfigProperty(name = "pipestream.registration.http.health-url")
    Optional<String> httpHealthUrl;

    @ConfigProperty(name = "pipestream.registration.http.tls-enabled", defaultValue = "false")
    boolean httpTlsEnabled;

    @ConfigProperty(name = "quarkus.http.port", defaultValue = "8080")
    int httpPort;

    @ConfigProperty(name = "quarkus.http.root-path", defaultValue = "")
    String httpRootPath;

    @ConfigProperty(name = "quarkus.application.version", defaultValue = "1.0.0")
    String version;

    @ConfigProperty(name = "quarkus.profile")
    String profile;

    private volatile String registeredServiceId;

    /**
     * Auto-register on startup if enabled
     */
    void onStart(@Observes StartupEvent ev) {
        if (!registrationEnabled) {
            LOG.info("Service registration disabled");
            return;
        }

        RegisterRequest request = buildRegisterRequest();
        registeredServiceId = ConsulRegistrar.generateServiceId(
            request.getName(),
            request.getConnectivity().getAdvertisedHost(),
            request.getConnectivity().getAdvertisedPort());

        LOG.infof("Self-registering %s with Consul (local handler)", serviceName);

        if (shouldCleanupOnStart()) {
            LOG.infof("Cleaning up existing registrations for %s before self-registration", serviceName);
            cleanupExistingRegistrations()
                .subscribe().with(
                    ignored -> startRegistration(request),
                    failure -> {
                        LOG.warnf(failure, "Failed to cleanup existing registrations for %s", serviceName);
                        startRegistration(request);
                    });
            return;
        }

        startRegistration(request);
    }

    /**
     * Deregister on shutdown to avoid stale entries.
     */
    void onStop(@Observes ShutdownEvent ev) {
        if (!registrationEnabled) {
            return;
        }

        String serviceId = registeredServiceId;
        if (serviceId == null || serviceId.isBlank()) {
            return;
        }

        LOG.infof("Deregistering %s from Consul on shutdown", serviceId);
        consulClient.deregisterService(serviceId)
            .subscribe().with(
                ignored -> LOG.infof("Deregistered %s from Consul", serviceId),
                failure -> LOG.warnf(failure, "Failed to deregister %s from Consul", serviceId)
            );
    }

    private void startRegistration(RegisterRequest request) {
        serviceRegistrationHandler.registerService(request)
            .subscribe().with(
                event -> {
                    LOG.infof("Self-registration event: %s - %s", event.getEventType(), event.getMessage());

                    if (event.getEventType() == PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED) {
                        LOG.infof("Successfully self-registered %s with Consul", serviceName);
                    } else if (event.getEventType() == PlatformEventType.PLATFORM_EVENT_TYPE_FAILED) {
                        LOG.errorf("Failed to self-register %s: %s", serviceName, event.getMessage());
                        if (event.hasErrorDetail()) {
                            LOG.error("Details: " + event.getErrorDetail());
                        }
                    }
                },
                throwable -> LOG.error("Self-registration failed", throwable),
                () -> LOG.debug("Self-registration stream completed")
            );
    }

    /**
     * Build registration request from configuration properties.
     * Uses the unified RegisterRequest with type=SERVICE_TYPE_SERVICE.
     */
    private RegisterRequest buildRegisterRequest() {
        // Build connectivity information
        Connectivity.Builder connectivity = Connectivity.newBuilder()
            .setAdvertisedHost(determineHost())
            .setAdvertisedPort(servicePort);

        if (!internalHost.isBlank()) {
            connectivity.setInternalHost(internalHost);
            connectivity.setInternalPort(internalPort > 0 ? internalPort : servicePort);
        }

        RegisterRequest.Builder builder = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setType(ServiceType.SERVICE_TYPE_SERVICE)
            .setConnectivity(connectivity.build())
            .setVersion(version);

        // Add metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("description", description);
        metadata.put("service-type", serviceType);
        metadata.put("profile", profile);
        builder.putAllMetadata(metadata);

        // Add capabilities
        if (!capabilities.isEmpty()) {
            Arrays.stream(capabilities.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(builder::addCapabilities);
        }

        // Add tags
        if (!tags.isEmpty()) {
            Arrays.stream(tags.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(builder::addTags);
        }

        addHttpEndpoint(builder);
        addGrpcServices(builder);

        return builder.build();
    }

    /**
     * Determine the host to register with
     */
    private String determineHost() {
        // Check for override environment variable
        String envHost = System.getenv("PLATFORM_REGISTRATION_HOST");
        if (envHost != null && !envHost.isEmpty()) {
            LOG.infof("Using PLATFORM_REGISTRATION_HOST from environment: %s", envHost);
            return envHost;
        }

        // Use configured host from properties
        LOG.infof("Using configured service host: %s", serviceHost);
        return serviceHost;
    }

    private Uni<Void> cleanupExistingRegistrations() {
        if (serviceName == null || serviceName.isBlank()) {
            return Uni.createFrom().voidItem();
        }

        return consulClient.healthServiceNodes(serviceName, false)
            .onItem().transformToUni(serviceEntries -> {
                if (serviceEntries == null || serviceEntries.getList() == null || serviceEntries.getList().isEmpty()) {
                    return Uni.createFrom().voidItem();
                }

                List<Uni<Void>> removals = new ArrayList<>();
                serviceEntries.getList().forEach(entry -> {
                    String serviceId = entry.getService() != null ? entry.getService().getId() : null;
                    if (serviceId == null || serviceId.isBlank()) {
                        return;
                    }
                    removals.add(consulClient.deregisterService(serviceId)
                        .onItem().invoke(ignored -> LOG.infof("Deregistered stale service instance: %s", serviceId))
                        .onFailure().invoke(failure ->
                            LOG.warnf(failure, "Failed to deregister stale service instance: %s", serviceId))
                        .replaceWithVoid());
                });

                if (removals.isEmpty()) {
                    return Uni.createFrom().voidItem();
                }

                return Uni.combine().all().unis(removals).discardItems();
            })
            .onFailure().invoke(failure ->
                LOG.warnf(failure, "Failed to cleanup existing registrations for %s", serviceName))
            .replaceWithVoid();
    }

    private boolean shouldCleanupOnStart() {
        return !isProdProfile(profile);
    }

    private boolean isProdProfile(String activeProfile) {
        if (activeProfile == null) {
            return false;
        }
        String normalized = activeProfile.trim().toLowerCase();
        return "prod".equals(normalized) || "production".equals(normalized);
    }

    private void addHttpEndpoint(RegisterRequest.Builder builder) {
        if (!httpRegistrationEnabled) {
            return;
        }

        String scheme = httpScheme;
        String host = httpAdvertisedHost
                .filter(value -> !value.isBlank())
                .orElse(serviceHost);
        int port = httpAdvertisedPort.orElse(httpPort);
        String basePath = httpBasePath
                .filter(value -> !value.isBlank())
                .orElse(httpRootPath == null ? "" : httpRootPath);
        String healthPath = httpHealthPath;
        boolean tlsEnabled = httpTlsEnabled;

        final HealthUrlOverride override;
        if (httpHealthUrl.isPresent()) {
            String rawHealthUrl = httpHealthUrl.get();
            override = parseHealthUrl(rawHealthUrl, port);
            if (override != null) {
                scheme = override.scheme();
                host = override.host();
                port = override.port();
                healthPath = override.healthPath();
                if ("https".equalsIgnoreCase(scheme)) {
                    tlsEnabled = true;
                }
            } else {
                healthPath = rawHealthUrl;
            }
        }

        builder.addHttpEndpoints(HttpEndpoint.newBuilder()
                .setScheme(scheme)
                .setHost(host)
                .setPort(port)
                .setBasePath(basePath)
                .setHealthPath(healthPath)
                .setTlsEnabled(tlsEnabled)
                .build());
    }

    private void addGrpcServices(RegisterRequest.Builder builder) {
        List<String> grpcServices = collectGrpcServices();
        if (!grpcServices.isEmpty()) {
            builder.addAllGrpcServices(grpcServices);
        }
    }

    private List<String> collectGrpcServices() {
        try {
            List<GrpcServerRecorder.GrpcServiceDefinition> definitions = GrpcServerRecorder.getServices();
            if (definitions == null || definitions.isEmpty()) {
                return List.of();
            }
            return definitions.stream()
                    .map(definition -> definition.definition.getServiceDescriptor().getName())
                    .filter(name -> name != null && !name.isBlank())
                    .filter(name -> !RESERVED_GRPC_SERVICES.contains(name))
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
        } catch (Exception | NoClassDefFoundError e) {
            LOG.debug("gRPC services are unavailable for registration", e);
            return List.of();
        }
    }

    private HealthUrlOverride parseHealthUrl(String rawHealthUrl, int fallbackPort) {
        if (rawHealthUrl == null || rawHealthUrl.isBlank()) {
            return null;
        }
        try {
            URI uri = URI.create(rawHealthUrl);
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null || host == null) {
                return null;
            }
            int port = uri.getPort();
            int resolvedPort = port != -1 ? port : fallbackPort;
            String path = uri.getRawPath();
            if (path == null || path.isBlank()) {
                path = "/";
            }
            String query = uri.getRawQuery();
            if (query != null && !query.isBlank()) {
                path = path + "?" + query;
            }
            return new HealthUrlOverride(scheme, host, resolvedPort, path);
        } catch (IllegalArgumentException e) {
            LOG.warnf("Invalid health-url '%s'; treating as health-path override.", rawHealthUrl);
            return null;
        }
    }

    private record HealthUrlOverride(String scheme, String host, int port, String healthPath) {
    }
}
