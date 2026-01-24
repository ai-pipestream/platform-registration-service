package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetServiceResponse;
import ai.pipestream.platform.registration.v1.HttpEndpoint;
import ai.pipestream.platform.registration.v1.ResolveServiceRequest;
import ai.pipestream.platform.registration.v1.ResolveServiceResponse;
import ai.pipestream.test.support.ConsulTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.ext.consul.ConsulClient;
import io.vertx.ext.consul.ServiceOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for ServiceDiscoveryHandler using real Consul.
 * This test validates that HTTP endpoint metadata is correctly stored in and retrieved from Consul
 * via ConsulTestResource which starts a real Consul container.
 */
@QuarkusTest
@QuarkusTestResource(ConsulTestResource.class)
class ServiceDiscoveryHandlerConsulIntegrationTest {

    @Inject
    ServiceDiscoveryHandler serviceDiscoveryHandler;

    @Inject
    ConsulClient consulClient;  // Real client, NOT mocked!

    @Test
    void resolveService_withHttpEndpointsInRealConsul_returnsEndpoints() {
        // Arrange - Create a unique service name to avoid conflicts
        String serviceName = "test-consul-http-service-" + UUID.randomUUID().toString().substring(0, 8);
        String serviceId = serviceName + "-id";

        // First, register a service with HTTP endpoints in real Consul
        var metadata = new java.util.HashMap<String, String>();
        metadata.put("http_endpoint_count", "2");
        metadata.put("http_endpoint_0_scheme", "https");
        metadata.put("http_endpoint_0_host", "api.example.com");
        metadata.put("http_endpoint_0_port", "443");
        metadata.put("http_endpoint_0_base_path", "/api/v1");
        metadata.put("http_endpoint_0_health_path", "/health");
        metadata.put("http_endpoint_0_tls_enabled", "true");
        metadata.put("http_endpoint_1_scheme", "http");
        metadata.put("http_endpoint_1_host", "internal.example.com");
        metadata.put("http_endpoint_1_port", "8080");
        metadata.put("http_endpoint_1_base_path", "/internal");
        metadata.put("http_endpoint_1_health_path", "/status");
        metadata.put("http_endpoint_1_tls_enabled", "false");
        metadata.put("http_schema_artifact_id", serviceName + "-http");
        metadata.put("http_schema_version", "1.0.0");

        ServiceOptions serviceOptions = new ServiceOptions()
                .setId(serviceId)
                .setName(serviceName)
                .setAddress("localhost")
                .setPort(8080)
                .setMeta(metadata);

        consulClient.registerService(serviceOptions)
            .await().atMost(Duration.ofSeconds(10));

        // Act - Resolve the service
        ResolveServiceRequest request = ResolveServiceRequest.newBuilder()
            .setServiceName(serviceName)
            .build();

        ResolveServiceResponse response = serviceDiscoveryHandler.resolveService(request)
            .await().atMost(Duration.ofSeconds(10));

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Service name should match", response.getServiceName(), is(equalTo(serviceName)));
        assertThat("Response should contain HTTP endpoints", response.getHttpEndpointsCount(), is(equalTo(2)));

        // Verify first endpoint
        HttpEndpoint endpoint0 = response.getHttpEndpoints(0);
        assertThat("First endpoint scheme should match", endpoint0.getScheme(), is(equalTo("https")));
        assertThat("First endpoint host should match", endpoint0.getHost(), is(equalTo("api.example.com")));
        assertThat("First endpoint port should match", endpoint0.getPort(), is(equalTo(443)));
        assertThat("First endpoint base path should match", endpoint0.getBasePath(), is(equalTo("/api/v1")));
        assertThat("First endpoint health path should match", endpoint0.getHealthPath(), is(equalTo("/health")));
        assertThat("First endpoint TLS should be enabled", endpoint0.getTlsEnabled(), is(true));

        // Verify second endpoint
        HttpEndpoint endpoint1 = response.getHttpEndpoints(1);
        assertThat("Second endpoint scheme should match", endpoint1.getScheme(), is(equalTo("http")));
        assertThat("Second endpoint host should match", endpoint1.getHost(), is(equalTo("internal.example.com")));
        assertThat("Second endpoint port should match", endpoint1.getPort(), is(equalTo(8080)));
        assertThat("Second endpoint base path should match", endpoint1.getBasePath(), is(equalTo("/internal")));
        assertThat("Second endpoint health path should match", endpoint1.getHealthPath(), is(equalTo("/status")));
        assertThat("Second endpoint TLS should be disabled", endpoint1.getTlsEnabled(), is(false));

        // Verify schema metadata
        assertThat("Should include HTTP schema artifact ID", response.hasHttpSchemaArtifactId(), is(true));
        assertThat("HTTP schema artifact ID should match", response.getHttpSchemaArtifactId(), is(equalTo(serviceName + "-http")));
        assertThat("Should include HTTP schema version", response.hasHttpSchemaVersion(), is(true));
        assertThat("HTTP schema version should match", response.getHttpSchemaVersion(), is(equalTo("1.0.0")));

        // Cleanup - deregister the service
        consulClient.deregisterService(serviceId)
            .await().atMost(Duration.ofSeconds(5));
    }

    @Test
    void resolveService_withoutHttpEndpointsInRealConsul_returnsEmpty() {
        // Arrange - Register a service without HTTP endpoints
        String serviceName = "test-consul-no-http-service-" + UUID.randomUUID().toString().substring(0, 8);
        String serviceId = serviceName + "-id";

        ServiceOptions serviceOptions = new ServiceOptions()
                .setId(serviceId)
                .setName(serviceName)
                .setAddress("localhost")
                .setPort(8080);
                // No HTTP metadata

        consulClient.registerService(serviceOptions)
            .await().atMost(Duration.ofSeconds(10));

        // Act
        ResolveServiceRequest request = ResolveServiceRequest.newBuilder()
            .setServiceName(serviceName)
            .build();

        ResolveServiceResponse response = serviceDiscoveryHandler.resolveService(request)
            .await().atMost(Duration.ofSeconds(10));

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Service name should match", response.getServiceName(), is(equalTo(serviceName)));
        assertThat("Response should have no HTTP endpoints", response.getHttpEndpointsCount(), is(equalTo(0)));
        assertThat("Should not have HTTP schema artifact ID", response.hasHttpSchemaArtifactId(), is(false));
        assertThat("Should not have HTTP schema version", response.hasHttpSchemaVersion(), is(false));

        // Cleanup
        consulClient.deregisterService(serviceId)
            .await().atMost(Duration.ofSeconds(5));
    }

    @Test
    void listServices_withHttpEndpointsInRealConsul_includesEndpoints() {
        // Arrange - Register a service with HTTP endpoints
        String serviceName = "test-consul-list-service-" + UUID.randomUUID().toString().substring(0, 8);
        String serviceId = serviceName + "-id";

        var metadata = new java.util.HashMap<String, String>();
        metadata.put("http_endpoint_count", "1");
        metadata.put("http_endpoint_0_scheme", "https");
        metadata.put("http_endpoint_0_host", "api.example.com");
        metadata.put("http_endpoint_0_port", "443");
        metadata.put("http_endpoint_0_base_path", "/api");
        metadata.put("http_endpoint_0_health_path", "/health");
        metadata.put("http_endpoint_0_tls_enabled", "true");

        ServiceOptions serviceOptions = new ServiceOptions()
                .setId(serviceId)
                .setName(serviceName)
                .setAddress("localhost")
                .setPort(8080)
                .setMeta(metadata);

        consulClient.registerService(serviceOptions)
            .await().atMost(Duration.ofSeconds(10));

        // Act - List all services
        var listResponse = serviceDiscoveryHandler.listServices()
            .await().atMost(Duration.ofSeconds(10));

        // Assert - Find our service in the list
        var ourService = listResponse.getServicesList().stream()
            .filter(service -> service.getServiceName().equals(serviceName))
            .findFirst();

        assertThat("Our service should be in the list", ourService.isPresent(), is(true));

        GetServiceResponse serviceResponse = ourService.get();
        assertThat("Service should have HTTP endpoints", serviceResponse.getHttpEndpointsCount(), is(equalTo(1)));

        HttpEndpoint endpoint = serviceResponse.getHttpEndpoints(0);
        assertThat("Endpoint scheme should match", endpoint.getScheme(), is(equalTo("https")));
        assertThat("Endpoint host should match", endpoint.getHost(), is(equalTo("api.example.com")));
        assertThat("Endpoint port should match", endpoint.getPort(), is(equalTo(443)));
        assertThat("Endpoint base path should match", endpoint.getBasePath(), is(equalTo("/api")));
        assertThat("Endpoint health path should match", endpoint.getHealthPath(), is(equalTo("/health")));
        assertThat("Endpoint TLS should be enabled", endpoint.getTlsEnabled(), is(true));

        // Cleanup
        consulClient.deregisterService(serviceId)
            .await().atMost(Duration.ofSeconds(5));
    }
}