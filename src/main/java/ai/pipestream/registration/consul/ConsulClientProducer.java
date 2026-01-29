package ai.pipestream.registration.consul;

import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ConsulClientProducer {
    
    private static final Logger LOG = Logger.getLogger(ConsulClientProducer.class);
    
    @Inject
    Vertx vertx;
    
    /**
     * Produces the ConsulClient bean for dependency injection.
     * @return the configured ConsulClient
     */
    @Produces
    @ApplicationScoped
    public ConsulClient produceConsulClient() {
        var config = ConfigProvider.getConfig();
        String consulHost = config.getOptionalValue("pipeline.consul.host", String.class).orElse("localhost");
        int consulPort = config.getOptionalValue("pipeline.consul.port", Integer.class).orElse(8500);
        LOG.infof("Creating Consul client for %s:%d", consulHost, consulPort);
        
        ConsulClientOptions options = new ConsulClientOptions()
            .setHost(consulHost)
            .setPort(consulPort);
            
        return ConsulClient.create(vertx, options);
    }
}
