package ai.pipestream.registration.health;

import ai.pipestream.registration.repository.ApicurioRegistryClient;
import io.vertx.mutiny.ext.consul.ConsulClient;
import io.vertx.mutiny.sqlclient.Pool;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import java.time.Duration;

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
 */
@Readiness
@ApplicationScoped
public class DependentServicesHealthCheck implements HealthCheck {

    @Inject
    Pool databasePool;
    
    @Inject
    ConsulClient consulClient;
    
    @Inject
    ApicurioRegistryClient apicurioClient;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named("dependent-services")
                .up();

        // Check Database (reactive)
        checkDatabase(responseBuilder);
        
        // Check Consul
        checkConsul(responseBuilder);
        
        // Check Apicurio Registry
        checkApicurio(responseBuilder);

        return responseBuilder.build();
    }

    private void checkDatabase(HealthCheckResponseBuilder builder) {
        try {
            // Use reactive database client to check connection
            databasePool.query("SELECT 1")
                    .execute()
                    .await()
                    .atMost(Duration.ofSeconds(2));
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
            // Check Consul agent connectivity using Mutiny API
            consulClient.agentInfo()
                    .await()
                    .atMost(Duration.ofSeconds(2));
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
            // Check Apicurio Registry health
            Boolean isHealthy = apicurioClient.isHealthy()
                    .await()
                    .atMost(Duration.ofSeconds(2));
            
            if (Boolean.TRUE.equals(isHealthy)) {
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