package ai.pipestream.registration.test.support;

import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * WireMock container resource with Stork static discovery for schema tests.
 */
public class SchemaWireMockTestResource extends WireMockContainerTestResource {

    @Override
    protected Map<String, String> buildConfig(GenericContainer<?> container) {
        Map<String, String> config = new HashMap<>(super.buildConfig(container));

        String moduleAddress = getHost() + ":" + getMappedPort(DEFAULT_HTTP_PORT);
        config.put("stork.elasticsearch-sink.service-discovery.type", "static");
        config.put("stork.elasticsearch-sink.service-discovery.address-list", moduleAddress);

        // Route a fake module to an unused port to force a NOT_FOUND path.
        config.put("stork.non-existent-module.service-discovery.type", "static");
        config.put("stork.non-existent-module.service-discovery.address-list", "127.0.0.1:1");

        return config;
    }
}
