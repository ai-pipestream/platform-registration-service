package ai.pipestream.dynamic.grpc.client.discovery;

/**
 * Interface for producing ServiceDiscovery instances.
 * <p>
 * Implementations of this interface are responsible for creating and configuring
 * appropriate ServiceDiscovery instances based on the application's configuration.
 * This allows for different service discovery mechanisms (e.g., Consul, Stork, etc.)
 * to be plugged in based on runtime configuration.
 * </p>
 */
public interface ServiceDiscoveryProducer {
    
    /**
     * Produces a ServiceDiscovery instance based on the current configuration.
     * <p>
     * The implementation should examine the application configuration to determine
     * which type of ServiceDiscovery to instantiate and return. This method is
     * typically called by CDI to provide a ServiceDiscovery bean for injection
     * throughout the application.
     * </p>
     * 
     * @return A configured ServiceDiscovery instance suitable for the current environment
     */
    ServiceDiscovery produceServiceDiscovery();
}