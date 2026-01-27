package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.Connectivity;
import ai.pipestream.platform.registration.v1.HttpEndpoint;
import ai.pipestream.platform.registration.v1.PlatformEventType;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.ServiceType;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.test.support.WireMockContainerTestResource;
import ai.pipestream.test.support.ConsulTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for ServiceRegistrationHandler HTTP schema registration.
 * This test validates that HTTP schemas are properly registered with Apicurio Registry
 * via ConsulTestResource (for Consul) and DevServices (for Apicurio).
 * 
 * Note: Uses a real Consul container + pipestream-wiremock-server container.
 * Health checks are HTTP-based to validate Consul reachability and schema registration.
 */
@QuarkusTest
@QuarkusTestResource(ConsulTestResource.class)
@QuarkusTestResource(WireMockContainerTestResource.class)
class ServiceRegistrationHandlerHttpIntegrationTest {

    @Inject
    ServiceRegistrationHandler serviceRegistrationHandler;

    @Inject
    ApicurioRegistryClient apicurioClient;  // Real client, NOT mocked!

    @ConfigProperty(name = "pipestream.test.wiremock.host")
    String wiremockHost;

    @ConfigProperty(name = "pipestream.test.wiremock.http-port")
    int wiremockHttpPort;

    @ConfigProperty(name = "pipestream.test.wiremock.container-ip")
    String wiremockContainerIp;

    @ConfigProperty(name = "pipestream.test.wiremock.container-http-port")
    int wiremockContainerPort;

    private String fetchOpenApiSchema() {
        String url = "http://" + wiremockHost + ":" + wiremockHttpPort + "/openapi.json";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
        try {
            HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());
            assertThat("WireMock OpenAPI should be reachable", response.statusCode(), is(200));
            return response.body();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to fetch OpenAPI schema from WireMock at " + url, e);
        }
    }

    @Test
    void registerService_withHttpSchema_registersInApicurio() {
        // Arrange - Create a unique service name to avoid conflicts
        String serviceName = "test-http-service-" + UUID.randomUUID().toString().substring(0, 8);
        String version = "1.0.0";
        String httpSchema = fetchOpenApiSchema();

        RegisterRequest request = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setType(ServiceType.SERVICE_TYPE_SERVICE)
            .setVersion(version)
            .setHttpSchema(httpSchema)
            .setHttpSchemaArtifactId(serviceName + "-http")
            .setHttpSchemaVersion(version)
            .setConnectivity(Connectivity.newBuilder()
                .setAdvertisedHost(wiremockHost)
                .setAdvertisedPort(wiremockHttpPort)
                .setInternalHost(wiremockContainerIp)
                .setInternalPort(wiremockContainerPort)
                .build())
            .addHttpEndpoints(HttpEndpoint.newBuilder()
                .setScheme("http")
                .setHost(wiremockContainerIp)
                .setPort(wiremockContainerPort)
                .setHealthPath("/q/health")
                .build())
            .build();

        // Act - Register the service and wait for completion
        var registrationEvents = serviceRegistrationHandler.registerService(request)
            .collect().asList()
            .await().atMost(Duration.ofSeconds(30));

        // Assert - Service should be registered successfully
        assertThat("Registration should complete", registrationEvents.size(), is(greaterThan(0)));
        assertThat("Should report Consul healthy",
            registrationEvents.stream()
                .anyMatch(event -> event.getEventType() == PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY),
            is(true));
        var completedEvent = registrationEvents.get(registrationEvents.size() - 1);
        assertThat("Last event should be completion", completedEvent.getEventType(),
            is(equalTo(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED)));

        // Verify the HTTP schema was registered in Apicurio
        String expectedArtifactId = serviceName + "-http-config-v1_0_0";
        String retrievedSchema = apicurioClient.getSchemaByArtifactId(expectedArtifactId, version)
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
        String httpSchema = fetchOpenApiSchema();

        RegisterRequest request = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setType(ServiceType.SERVICE_TYPE_SERVICE)
            .setVersion(version)
            .setHttpSchema(httpSchema)
            // No explicit http_schema_artifact_id - should use default
            .setConnectivity(Connectivity.newBuilder()
                .setAdvertisedHost(wiremockHost)
                .setAdvertisedPort(wiremockHttpPort)
                .setInternalHost(wiremockContainerIp)
                .setInternalPort(wiremockContainerPort)
                .build())
            .addHttpEndpoints(HttpEndpoint.newBuilder()
                .setScheme("http")
                .setHost(wiremockContainerIp)
                .setPort(wiremockContainerPort)
                .setHealthPath("/q/health")
                .build())
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
        String retrievedSchema = apicurioClient.getSchemaByArtifactId(expectedArtifactId, version)
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
        String httpSchema = fetchOpenApiSchema();

        RegisterRequest request = RegisterRequest.newBuilder()
            .setName(serviceName)
            .setType(ServiceType.SERVICE_TYPE_SERVICE)
            .setVersion(serviceVersion)
            .setHttpSchema(httpSchema)
            .setHttpSchemaArtifactId(serviceName + "-openapi")
            // No http_schema_version - should use service version
            .setConnectivity(Connectivity.newBuilder()
                .setAdvertisedHost(wiremockHost)
                .setAdvertisedPort(wiremockHttpPort)
                .setInternalHost(wiremockContainerIp)
                .setInternalPort(wiremockContainerPort)
                .build())
            .addHttpEndpoints(HttpEndpoint.newBuilder()
                .setScheme("http")
                .setHost(wiremockContainerIp)
                .setPort(wiremockContainerPort)
                .setHealthPath("/q/health")
                .build())
            .build();

        // Act
        var registrationResult = serviceRegistrationHandler.registerService(request)
            .collect().last()
            .await().atMost(Duration.ofSeconds(30));

        // Assert
        assertThat("Registration should succeed", registrationResult, is(notNullValue()));

        // Verify schema registered with service version
        String expectedArtifactId = serviceName + "-openapi-config-v3_1_0";
        String retrievedSchema = apicurioClient.getSchemaByArtifactId(expectedArtifactId, serviceVersion)
            .await().atMost(Duration.ofSeconds(10));

        assertThat("HTTP schema should use service version",
            retrievedSchema, is(notNullValue()));
    }
}
