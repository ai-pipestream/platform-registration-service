package ai.pipestream.dynamic.grpc.client.discovery;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer for ServiceDiscovery when running the dynamic-grpc module standalone.
 * 
 * This producer provides a default ServiceDiscovery implementation when no other
 * implementation is available (e.g., when running without the engine module).
 * 
 * The @DefaultBean annotation ensures this is only used when no other ServiceDiscovery
 * bean is available, allowing the engine to provide its own implementation.
 */
@ApplicationScoped
public class StandaloneServiceDiscoveryProducer {
    /**
     * Default constructor for CDI.
     */
    public StandaloneServiceDiscoveryProducer() { }
    
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneServiceDiscoveryProducer.class);
    
    @Inject
    @ServiceDiscoveryImpl(ServiceDiscoveryImpl.Type.CONSUL_DIRECT)
    DynamicConsulServiceDiscovery dynamicConsulServiceDiscovery;
    
    /**
     * Produces a default ServiceDiscovery implementation for standalone usage.
     * 
     * When running with the engine module, the engine's ServiceDiscoveryProducer
     * will take precedence over this default.
     * 
     * @return ServiceDiscovery implementation
     */
    @Produces
    @DefaultBean
    @ApplicationScoped
    public ServiceDiscovery produceServiceDiscovery() {
        LOG.info("Producing default ServiceDiscovery for standalone dynamic-grpc module");
        return dynamicConsulServiceDiscovery;
    }
}