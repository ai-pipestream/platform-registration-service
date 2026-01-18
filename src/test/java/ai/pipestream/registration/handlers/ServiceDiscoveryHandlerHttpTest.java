package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.HttpEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Unit tests for ServiceDiscoveryHandler HTTP endpoint parsing.
 * Tests the parseHttpEndpoints method that extracts HTTP endpoint information from Consul metadata.
 */
@QuarkusTest
class ServiceDiscoveryHandlerHttpTest {

    @Inject
    ServiceDiscoveryHandler serviceDiscoveryHandler;

    @Test
    void parseHttpEndpoints_parsesCompleteEndpointData() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
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

        // Act
        List<HttpEndpoint> endpoints = serviceDiscoveryHandler.parseHttpEndpoints(metadata);

        // Assert
        assertThat("Should parse two HTTP endpoints", endpoints.size(), is(2));

        // Verify first endpoint
        HttpEndpoint endpoint0 = endpoints.get(0);
        assertThat("First endpoint scheme should match", endpoint0.getScheme(), is(equalTo("https")));
        assertThat("First endpoint host should match", endpoint0.getHost(), is(equalTo("api.example.com")));
        assertThat("First endpoint port should match", endpoint0.getPort(), is(equalTo(443)));
        assertThat("First endpoint base path should match", endpoint0.getBasePath(), is(equalTo("/api/v1")));
        assertThat("First endpoint health path should match", endpoint0.getHealthPath(), is(equalTo("/health")));
        assertThat("First endpoint TLS should be enabled", endpoint0.getTlsEnabled(), is(true));

        // Verify second endpoint
        HttpEndpoint endpoint1 = endpoints.get(1);
        assertThat("Second endpoint scheme should match", endpoint1.getScheme(), is(equalTo("http")));
        assertThat("Second endpoint host should match", endpoint1.getHost(), is(equalTo("internal.example.com")));
        assertThat("Second endpoint port should match", endpoint1.getPort(), is(equalTo(8080)));
        assertThat("Second endpoint base path should match", endpoint1.getBasePath(), is(equalTo("/internal")));
        assertThat("Second endpoint health path should match", endpoint1.getHealthPath(), is(equalTo("/status")));
        assertThat("Second endpoint TLS should be disabled", endpoint1.getTlsEnabled(), is(false));
    }

    @Test
    void parseHttpEndpoints_handlesMissingCount() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        // No http_endpoint_count

        // Act
        List<HttpEndpoint> endpoints = serviceDiscoveryHandler.parseHttpEndpoints(metadata);

        // Assert
        assertThat("Should return empty list when count is missing", endpoints.isEmpty(), is(true));
    }

    @Test
    void parseHttpEndpoints_handlesInvalidCount() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("http_endpoint_count", "invalid");

        // Act
        List<HttpEndpoint> endpoints = serviceDiscoveryHandler.parseHttpEndpoints(metadata);

        // Assert
        assertThat("Should return empty list when count is invalid", endpoints.isEmpty(), is(true));
    }

    @Test
    void parseHttpEndpoints_handlesPartialEndpointData() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("http_endpoint_count", "1");
        metadata.put("http_endpoint_0_scheme", "http");
        // Missing other fields - should use defaults

        // Act
        List<HttpEndpoint> endpoints = serviceDiscoveryHandler.parseHttpEndpoints(metadata);

        // Assert
        assertThat("Should parse endpoint with defaults", endpoints.size(), is(1));

        HttpEndpoint endpoint = endpoints.get(0);
        assertThat("Scheme should match", endpoint.getScheme(), is(equalTo("http")));
        assertThat("Host should be empty", endpoint.getHost(), is(emptyString()));
        assertThat("Port should be 0", endpoint.getPort(), is(0));
        assertThat("Base path should be empty", endpoint.getBasePath(), is(emptyString()));
        assertThat("Health path should be empty", endpoint.getHealthPath(), is(emptyString()));
        assertThat("TLS should be disabled by default", endpoint.getTlsEnabled(), is(false));
    }

    @Test
    void parseHttpEndpoints_handlesZeroCount() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("http_endpoint_count", "0");

        // Act
        List<HttpEndpoint> endpoints = serviceDiscoveryHandler.parseHttpEndpoints(metadata);

        // Assert
        assertThat("Should return empty list when count is zero", endpoints.isEmpty(), is(true));
    }

    @Test
    void parseHttpEndpoints_ignoresMalformedEndpointData() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("http_endpoint_count", "2");
        metadata.put("http_endpoint_0_scheme", "https");
        metadata.put("http_endpoint_0_host", "valid.example.com");
        metadata.put("http_endpoint_0_port", "not-a-number"); // Invalid port
        metadata.put("http_endpoint_0_base_path", "/api");
        // Second endpoint completely missing

        // Act
        List<HttpEndpoint> endpoints = serviceDiscoveryHandler.parseHttpEndpoints(metadata);

        // Assert
        assertThat("Should parse available endpoint data", endpoints.size(), is(1));

        HttpEndpoint endpoint = endpoints.get(0);
        assertThat("Scheme should match", endpoint.getScheme(), is(equalTo("https")));
        assertThat("Host should match", endpoint.getHost(), is(equalTo("valid.example.com")));
        assertThat("Port should default to 0 for invalid value", endpoint.getPort(), is(0));
        assertThat("Base path should match", endpoint.getBasePath(), is(equalTo("/api")));
    }
}