package ai.pipestream.dynamic.grpc.util;

import org.jboss.logging.Logger;

/**
 * Simple utility to resolve hostname for service registration.
 */
public class HostnameResolver {
    /**
     * Utility class, not meant to be instantiated.
     */
    private HostnameResolver() { }
    
    private static final Logger LOG = Logger.getLogger(HostnameResolver.class);
    
    /**
     * Resolve hostname using environment variables and fallbacks
     * 
     * @param serviceName optional service name for service-specific env var
     * @return resolved hostname
     */
    public static String resolveHostname(String serviceName) {
        try {
            // 1. Try service-specific environment variable (e.g. MAPPING_SERVICE_HOST)
            if (serviceName != null && !serviceName.isEmpty()) {
                String serviceEnvVar = serviceName.toUpperCase().replace("-", "_") + "_HOST";
                String serviceHost = System.getenv(serviceEnvVar);
                if (serviceHost != null && !serviceHost.isEmpty()) {
                    LOG.infof("Using service-specific hostname from %s: %s", serviceEnvVar, serviceHost);
                    return serviceHost;
                }
            }
            
            // 2. Try generic SERVICE_HOST environment variable
            String genericHost = System.getenv("SERVICE_HOST");
            if (genericHost != null && !genericHost.isEmpty()) {
                LOG.infof("Using generic SERVICE_HOST: %s", genericHost);
                return genericHost;
            }
            
            // 3. Try container hostname (Docker/K8s)
            String containerHost = System.getenv("HOSTNAME");
            if (containerHost != null && !containerHost.isEmpty()) {
                LOG.infof("Using container HOSTNAME: %s", containerHost);
                return containerHost;
            }
            
            // 4. Fallback to localhost
            LOG.info("Using fallback hostname: localhost");
            return "localhost";
            
        } catch (Exception e) {
            LOG.warnf(e, "Error resolving hostname, using localhost fallback");
            return "localhost";
        }
    }
    
    /**
     * Resolve hostname with no service-specific lookup.
     *
     * @return resolved hostname string
     */
    public static String resolveHostname() {
        return resolveHostname(null);
    }
    
    /**
     * Log current hostname resolution for debugging.
     *
     * @param serviceName optional service name used to look up a service-specific host override
     */
    public static void logHostnameResolution(String serviceName) {
        String hostname = resolveHostname(serviceName);
        LOG.infof("=== HOSTNAME RESOLUTION DEBUG ===");
        LOG.infof("Service: %s", serviceName);
        LOG.infof("Resolved hostname: %s", hostname);
        
        // Log all relevant environment variables for debugging
        if (serviceName != null) {
            String serviceEnvVar = serviceName.toUpperCase().replace("-", "_") + "_HOST";
            LOG.infof("  %s = %s", serviceEnvVar, System.getenv(serviceEnvVar));
        }
        LOG.infof("  SERVICE_HOST = %s", System.getenv("SERVICE_HOST"));
        LOG.infof("  HOSTNAME = %s", System.getenv("HOSTNAME"));
        LOG.infof("=== END HOSTNAME RESOLUTION ===");
    }
}
