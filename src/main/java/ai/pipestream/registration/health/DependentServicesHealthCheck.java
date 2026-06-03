package ai.pipestream.registration.health;

import ai.pipestream.registration.repository.ApicurioRegistryClient;
import io.vertx.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Health check for services that platform-registration-service directly depends on.
 * These checks are automatically exposed via:
 * - REST: /q/health/ready
 * - gRPC: grpc.health.v1.Health service
 * <p>
 * Services checked:
 * - PostgreSQL (service registry database)
 * - Consul (service discovery backend)
 * - Apicurio Registry (schema storage)
 *
 * <p>All checks are blocking. The database (JDBC query timeout) and Consul (future
 * timeout) checks are bounded to ~2s; the Apicurio check relies on the registry
 * client's own HTTP timeout rather than an explicit bound here.
 */
@Readiness
@ApplicationScoped
public class DependentServicesHealthCheck implements HealthCheck {

    @Inject
    DataSource dataSource;

    @Inject
    ConsulClient consulClient;

    @Inject
    ApicurioRegistryClient apicurioClient;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named("dependent-services")
                .up();

        // Check Database
        checkDatabase(responseBuilder);

        // Check Consul
        checkConsul(responseBuilder);

        // Check Apicurio Registry
        checkApicurio(responseBuilder);

        return responseBuilder.build();
    }

    private void checkDatabase(HealthCheckResponseBuilder builder) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(2);
            statement.execute("SELECT 1");
            builder.withData("database", "UP")
                   .withData("database-details", "Service registry database is accessible");
        } catch (Exception e) {
            builder.withData("database", "DOWN")
                   .withData("database-error", e.getMessage())
                   .down();
        }
    }

    private void checkConsul(HealthCheckResponseBuilder builder) {
        try {
            // Check Consul agent connectivity (bridged to blocking, bounded to 2s)
            consulClient.agentInfo()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);
            builder.withData("consul", "UP")
                   .withData("consul-details", "Connected to Consul agent");
        } catch (Exception e) {
            builder.withData("consul", "DOWN")
                   .withData("consul-error", "Failed to connect to Consul: " + e.getMessage())
                   .down();
        }
    }

    private void checkApicurio(HealthCheckResponseBuilder builder) {
        try {
            // Check Apicurio Registry health (blocking; never throws — returns false on failure)
            boolean isHealthy = apicurioClient.isHealthy();

            if (isHealthy) {
                builder.withData("apicurio", "UP")
                       .withData("apicurio-details", "Schema registry is accessible");
            } else {
                builder.withData("apicurio", "DOWN")
                       .withData("apicurio-error", "Schema registry health check failed")
                       .down();
            }
        } catch (Exception e) {
            builder.withData("apicurio", "DOWN")
                   .withData("apicurio-error", "Failed to check Apicurio health: " + e.getMessage())
                   .down();
        }
    }
}
