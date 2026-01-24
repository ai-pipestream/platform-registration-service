package ai.pipestream.registration.consul;

import ai.pipestream.platform.registration.v1.Connectivity;
import ai.pipestream.platform.registration.v1.HttpEndpoint;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.ServiceType;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.consul.CheckOptions;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.mutiny.ext.consul.ConsulClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConsulRegistrarTest {

    @Mock
    ConsulClient consulClient;

    ConsulRegistrar consulRegistrar;

    @BeforeEach
    void setup() {
        consulRegistrar = new ConsulRegistrar();
        consulRegistrar.consulClient = consulClient;
    }

    @Test
    void registerService_shouldUseHttpHealthCheck_whenHttpEndpointIsProvided() {
        // Arrange
        String serviceName = "my-http-service";
        String host = "10.0.0.1";
        int port = 8080;
        String serviceId = ConsulRegistrar.generateServiceId(serviceName, host, port);
        String healthPath = "/health";

        RegisterRequest request = RegisterRequest.newBuilder()
                .setName(serviceName)
                .setType(ServiceType.SERVICE_TYPE_SERVICE)
                .setVersion("1.0.0")
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedHost(host)
                        .setAdvertisedPort(port)
                        .build())
                .addHttpEndpoints(HttpEndpoint.newBuilder()
                        .setScheme("http")
                        .setHost(host)
                        .setPort(port)
                        .setHealthPath(healthPath)
                        .build())
                .build();

        when(consulClient.registerService(any(ServiceOptions.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Act
        consulRegistrar.registerService(request, serviceId).await().indefinitely();

        // Assert
        ArgumentCaptor<ServiceOptions> optionsCaptor = ArgumentCaptor.forClass(ServiceOptions.class);
        verify(consulClient).registerService(optionsCaptor.capture());

        ServiceOptions options = optionsCaptor.getValue();
        CheckOptions checkOptions = options.getCheckOptions();

        assertNotNull(checkOptions, "Check options should not be null");
        
        // THIS IS THE BUG REPRODUCTION ASSERTION
        // Current implementation uses setGrpc, so getHttp() will be null, and getGrpc() will be set.
        // We want the opposite.
        assertNull(checkOptions.getGrpc(), "Should NOT have gRPC check configured for HTTP service");
        assertNotNull(checkOptions.getHttp(), "Should have HTTP check configured");
        assertEquals("http://" + host + ":" + port + healthPath, checkOptions.getHttp());
    }
}
