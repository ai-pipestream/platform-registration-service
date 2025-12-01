package ai.pipestream.dynamic.grpc.client;

import io.smallrye.mutiny.Uni;
import io.smallrye.stork.Stork;
import io.smallrye.stork.api.Service;
import io.smallrye.stork.api.ServiceDefinition;
import io.smallrye.stork.api.ServiceInstance;
import io.smallrye.stork.spi.config.SimpleServiceConfig;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
/**
 * Manages dynamic definition of Stork services backed by Consul discovery
 * and provides access to discovered ServiceInstance lists.
 */
public class ServiceDiscoveryManager {

    /**
     * Default constructor for CDI frameworks.
     */
    public ServiceDiscoveryManager() { }

    private static final Logger LOG = Logger.getLogger(ServiceDiscoveryManager.class);

    @ConfigProperty(name = "pipeline.consul.host", defaultValue = "localhost")
    String consulHost;
    
    @ConfigProperty(name = "pipeline.consul.port", defaultValue = "8500")
    String consulPort;
    
    @ConfigProperty(name = "pipeline.consul.refresh-period", defaultValue = "10s")
    String consulRefreshPeriod;
    
    @ConfigProperty(name = "pipeline.consul.use-health-checks", defaultValue = "false")
    boolean consulUseHealthChecks;

    /**
     * Ensures a service is defined in Stork for discovery using the same Consul application name.
     *
     * @param serviceName the logical service name to define and discover via Consul
     * @return a Uni that completes when the service is defined or immediately if it already exists
     */
    public Uni<Void> ensureServiceDefined(String serviceName) {
        return ensureServiceDefinedFor(serviceName, serviceName);
    }

    /**
     * Ensures a service is defined in Stork under the given storkServiceName but discovers
     * Consul instances registered under consulApplicationName.
     *
     * @param storkServiceName the logical service name as it will be known to Stork
     * @param consulApplicationName the Consul service name to query for instances
     * @return a Uni that completes when the service is defined or fails if definition cannot be created
     */
    public Uni<Void> ensureServiceDefinedFor(String storkServiceName, String consulApplicationName) {
        Optional<Service> existingService = Stork.getInstance().getServiceOptional(storkServiceName);
        if (existingService.isPresent()) {
            LOG.debugf("Service %s already defined in Stork", storkServiceName);
            return Uni.createFrom().voidItem();
        }

        LOG.infof("Defining new Stork service for dynamic discovery: %s (consul application: %s)", storkServiceName, consulApplicationName);
        
        // Stork Consul discovery configuration
        Map<String, String> consulParams = new HashMap<>();
        consulParams.put("consul-host", consulHost);
        consulParams.put("consul-port", consulPort); 
        consulParams.put("refresh-period", consulRefreshPeriod);
        // Allow configuration to control whether Consul health checks are used
        consulParams.put("use-health-checks", String.valueOf(consulUseHealthChecks));
        
        // Allow overriding the Consul service name per Stork service via config
        // Property key: pipeline.consul.application-name.<storkServiceName>
        final Config config = ConfigProvider.getConfig();
        final String overrideKey = "pipeline.consul.application-name." + storkServiceName;
        final String applicationToDiscover = config.getOptionalValue(overrideKey, String.class)
                .orElse(consulApplicationName);
        // The "application" parameter is the Consul service name to look for
        consulParams.put("application", applicationToDiscover);

        var consulConfig = new SimpleServiceConfig.SimpleServiceDiscoveryConfig("consul", consulParams);
        ServiceDefinition definition = ServiceDefinition.of(consulConfig);

        try {
            Stork.getInstance().defineIfAbsent(storkServiceName, definition);
            LOG.infof("Successfully defined Stork service: %s with Consul discovery", storkServiceName);
            
            // Log what Stork will look for
            LOG.debugf("Stork will look for Consul service named: %s (overrideKey=%s, use-health-checks=%s)", 
                       applicationToDiscover, overrideKey, String.valueOf(consulUseHealthChecks));
            
            return Uni.createFrom().voidItem();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to define Stork service: %s", storkServiceName);
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Gets service instances for a given service name
     */
    public Uni<List<ServiceInstance>> getServiceInstances(String serviceName) {
        try {
            Service service = Stork.getInstance().getService(serviceName);
            return service.getInstances();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to get service instances for %s", serviceName);
            return Uni.createFrom().failure(e);
        }
    }
}
