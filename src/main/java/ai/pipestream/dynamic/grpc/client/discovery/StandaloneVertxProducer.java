package ai.pipestream.dynamic.grpc.client.discovery;

import io.quarkus.arc.DefaultBean;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer for Vertx when running the dynamic-grpc module standalone.
 * 
 * This producer provides a default Vertx instance when no other instance
 * is available (e.g., when running without Quarkus vertx extensions).
 * 
 * The @DefaultBean annotation ensures this is only used when no other Vertx
 * bean is available.
 */
@ApplicationScoped
public class StandaloneVertxProducer {
    
    /**
     * Default constructor for CDI.
     */
    public StandaloneVertxProducer() { }
    
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneVertxProducer.class);
    
    // Quarkus usually provides this, but we need it for standalone usage
    @Inject
    io.vertx.mutiny.core.Vertx mutinyVertx;
    
    /**
     * Produces a default Vertx instance for standalone usage.
     * 
     * @return Vertx instance
     */
    @Produces
    @DefaultBean
    @ApplicationScoped
    public Vertx produceVertx() {
        LOG.info("Producing default Vertx for standalone dynamic-grpc module");
        // Get the delegate from Mutiny Vertx
        return mutinyVertx.getDelegate();
    }
}