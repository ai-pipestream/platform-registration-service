package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.PlatformEventType;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for ServiceRegistrationHandler HTTP schema registration.
 * This test validates that HTTP schemas are properly registered with Apicurio Registry
 * via DevServices without mocking the client.
 */
@QuarkusTest
class ServiceRegistrationHandlerHttpIntegrationTest {

    @Inject
    ServiceRegistrationHandler serviceRegistrationHandler;

    @Inject
    ApicurioRegistryClient apicurioClient;  // Real client, NOT mocked!

    @Test
    void registerService_withHttpSchema_registersInApicurio() {
        // Arrange - Create a unique service name to avoid conflicts
        String serviceName = "test-http-service-" + UUID.randomUUID().toString().substring(0, 8);
        String version = "1.0.0";
        String httpSchema = """
            {
                "openapi": "3.0.0",
                "info": {
                    "title": "Test HTTP Service",
                    "version": "1.0.0"
                },
                "paths": {
                    "/health": {
                        "get": {
                            "responses": {
                                "200": {
                                    "description": "Health check"
                                }
                            }
                        }
                    }
                }
            }""";

        RegisterRequest request = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setVersion(version)
            .setHttpSchema(httpSchema)
            .setHttpSchemaArtifactId(serviceName + "-http")
            .setHttpSchemaVersion(version)
            .build();

        // Act - Register the service and wait for completion
        var registrationEvents = serviceRegistrationHandler.registerService(request)
            .collect().asList()
            .await().atMost(Duration.ofSeconds(30));

        // Assert - Service should be registered successfully
        assertThat("Registration should complete", registrationEvents.size(), is(greaterThan(0)));
        var completedEvent = registrationEvents.get(registrationEvents.size() - 1);
        assertThat("Last event should be completion", completedEvent.getEventType(),
            is(equalTo(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED)));

        // Verify the HTTP schema was registered in Apicurio
        String expectedArtifactId = serviceName + "-http-config-v1_0_0";
        String retrievedSchema = apicurioClient.getSchema(expectedArtifactId, version)
            .await().atMost(Duration.ofSeconds(10));

        assertThat("HTTP schema should be retrievable from Apicurio",
            retrievedSchema, is(notNullValue()));
        assertThat("Retrieved schema should match the registered schema",
            retrievedSchema, is(equalTo(httpSchema)));
    }

    @Test
    void registerService_withHttpSchema_usesDefaultArtifactId() {
        // Arrange - Test default artifact ID generation
        String serviceName = "test-default-http-service-" + UUID.randomUUID().toString().substring(0, 8);
        String version = "2.0.0";
        String httpSchema = "{\"openapi\": \"3.0.0\", \"info\": {\"title\": \"Default Test\"}}";

        RegisterRequest request = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setVersion(version)
            .setHttpSchema(httpSchema)
            // No explicit http_schema_artifact_id - should use default
            .build();

        // Act - Register the service and wait for completion
        var registrationEvents = serviceRegistrationHandler.registerService(request)
            .collect().asList()
            .await().atMost(Duration.ofSeconds(30));

        // Assert
        assertThat("Registration should complete", registrationEvents.size(), is(greaterThan(0)));
        var completedEvent = registrationEvents.get(registrationEvents.size() - 1);
        assertThat("Last event should be completion", completedEvent.getEventType(),
            is(equalTo(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED)));

        // Verify the HTTP schema was registered with default artifact ID
        String expectedArtifactId = serviceName + "-http-config-v2_0_0";
        String retrievedSchema = apicurioClient.getSchema(expectedArtifactId, version)
            .await().atMost(Duration.ofSeconds(10));

        assertThat("HTTP schema should be retrievable with default artifact ID",
            retrievedSchema, is(notNullValue()));
        assertThat("Retrieved schema should match",
            retrievedSchema, is(equalTo(httpSchema)));
    }

    @Test
    void registerService_withHttpSchema_usesServiceVersion() {
        // Arrange - Test using service version when no http_schema_version provided
        String serviceName = "test-version-http-service-" + UUID.randomUUID().toString().substring(0, 8);
        String serviceVersion = "3.1.0";
        String httpSchema = "{\"openapi\": \"3.0.0\", \"info\": {\"version\": \"3.1.0\"}}";

        RegisterRequest request = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setVersion(serviceVersion)
            .setHttpSchema(httpSchema)
            .setHttpSchemaArtifactId(serviceName + "-openapi")
            // No http_schema_version - should use service version
            .build();

        // Act
        var registrationResult = serviceRegistrationHandler.registerService(request)
            .collect().last()
            .await().atMost(Duration.ofSeconds(30));

        // Assert
        assertThat("Registration should succeed", registrationResult, is(notNullValue()));

        // Verify schema registered with service version
        String expectedArtifactId = serviceName + "-openapi-config-v3_1_0";
        String retrievedSchema = apicurioClient.getSchema(expectedArtifactId, serviceVersion)
            .await().atMost(Duration.ofSeconds(10));

        assertThat("HTTP schema should use service version",
            retrievedSchema, is(notNullValue()));
    }
}