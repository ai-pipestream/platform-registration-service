package ai.pipestream.registration.startup;

import ai.pipestream.platform.registration.v1.EventType;
import ai.pipestream.platform.registration.v1.RegisterServiceRequest;
import ai.pipestream.registration.handlers.ServiceRegistrationHandler;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles self-registration of the platform-registration-service with Consul.
 * This bypasses the gRPC client (which would create a circular dependency) and
 * calls the local ServiceRegistrationHandler directly.
 */
@ApplicationScoped
@io.quarkus.arc.properties.IfBuildProperty(name = "service.registration.enabled", stringValue = "true")
public class SelfRegistrationService {

    private static final Logger LOG = Logger.getLogger(SelfRegistrationService.class);

    @Inject
    ServiceRegistrationHandler serviceRegistrationHandler;

    @ConfigProperty(name = "service.registration.enabled", defaultValue = "false")
    boolean registrationEnabled;

    @ConfigProperty(name = "service.registration.service-name", defaultValue = "")
    String serviceName;

    @ConfigProperty(name = "service.registration.description", defaultValue = "")
    String description;

    @ConfigProperty(name = "service.registration.service-type", defaultValue = "APPLICATION")
    String serviceType;

    @ConfigProperty(name = "service.registration.host", defaultValue = "localhost")
    String serviceHost;

    @ConfigProperty(name = "service.registration.port", defaultValue = "0")
    int servicePort;

    @ConfigProperty(name = "service.registration.capabilities", defaultValue = "")
    String capabilities;

    @ConfigProperty(name = "service.registration.tags", defaultValue = "")
    String tags;

    @ConfigProperty(name = "quarkus.application.version", defaultValue = "1.0.0")
    String version;

    @ConfigProperty(name = "quarkus.profile")
    String profile;

    /**
     * Auto-register on startup if enabled
     */
    void onStart(@Observes StartupEvent ev) {
        if (!registrationEnabled) {
            LOG.info("Service registration disabled");
            return;
        }

        LOG.infof("Self-registering %s with Consul (local handler)", serviceName);

        RegisterServiceRequest request = buildServiceRequest();

        serviceRegistrationHandler.registerService(request)
            .subscribe().with(
                event -> {
                    LOG.infof("Self-registration event: %s - %s", event.getEventType(), event.getMessage());

                    if (event.getEventType() == EventType.EVENT_TYPE_COMPLETED) {
                        LOG.infof("Successfully self-registered %s with Consul", serviceName);
                    } else if (event.getEventType() == EventType.EVENT_TYPE_FAILED) {
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
     * Build service registration request from configuration properties
     */
    private RegisterServiceRequest buildServiceRequest() {
        RegisterServiceRequest.Builder builder = RegisterServiceRequest.newBuilder()
            .setServiceName(serviceName)
            .setHost(determineHost())
            .setPort(servicePort)
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
}
