package ai.pipestream.registration.test.support;

import ai.pipestream.test.support.BaseWireMockTestResource;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * Testcontainers resource for pipestream-wiremock-server with HTTP + direct gRPC ports exposed.
 * Provides host/port overrides for tests that need to call the mock HTTP service.
 */
public class WireMockContainerTestResource extends BaseWireMockTestResource {

    @Override
    protected Integer[] exposedPorts() {
        return new Integer[]{DEFAULT_HTTP_PORT, DEFAULT_GRPC_PORT};
    }

    @Override
    protected String readyLogPattern() {
        return ".*Direct Streaming gRPC Server started.*";
    }

    @Override
    protected Map<String, String> buildConfig(GenericContainer<?> container) {
        String host = getHost();
        String httpPort = String.valueOf(getMappedPort(DEFAULT_HTTP_PORT));
        String directPort = String.valueOf(getMappedPort(DEFAULT_GRPC_PORT));
        String containerIp = container.getContainerInfo()
            .getNetworkSettings()
            .getNetworks()
            .values()
            .iterator()
            .next()
            .getIpAddress();

        Map<String, String> config = new HashMap<>();
        config.put("pipestream.test.wiremock.host", host);
        config.put("pipestream.test.wiremock.http-port", httpPort);
        config.put("pipestream.test.wiremock.direct-port", directPort);
        config.put("pipestream.test.wiremock.container-ip", containerIp);
        config.put("pipestream.test.wiremock.container-http-port", String.valueOf(DEFAULT_HTTP_PORT));

        // Keep the existing registration-service defaults for callers that need it.
        config.put("pipestream.registration.registration-service.host", host);
        config.put("pipestream.registration.registration-service.port", directPort);
        return config;
    }
}
