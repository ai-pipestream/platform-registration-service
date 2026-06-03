package ai.pipestream.registration.discovery;

import io.vertx.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

/**
 * Exposes the plain (non-reactive) Vert.x {@link ConsulClient} as a CDI bean.
 *
 * <p>The {@code pipestream-service-registration} extension only produces the Mutiny
 * {@code io.vertx.mutiny.ext.consul.ConsulClient}. Discovery code prefers the standard
 * Vert.x client so it can bridge to blocking on a virtual thread via plain
 * {@code Future#toCompletionStage()} — no Mutiny on the call path. This producer simply
 * unwraps the Mutiny client's delegate, so both share the same underlying connection.
 */
@ApplicationScoped
public class ConsulClientProducer {

    @Inject
    io.vertx.mutiny.ext.consul.ConsulClient mutinyConsulClient;

    @Produces
    @ApplicationScoped
    public ConsulClient consulClient() {
        return mutinyConsulClient.getDelegate();
    }
}
