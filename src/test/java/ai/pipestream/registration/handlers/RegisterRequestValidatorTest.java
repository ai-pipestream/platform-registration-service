package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.Connectivity;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.ServiceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegisterRequestValidatorTest {

    private static RegisterRequest.Builder validBaseBuilder() {
        return RegisterRequest.newBuilder()
                .setName("test-service")
                .setVersion("1.0.0")
                .setType(ServiceType.SERVICE_TYPE_SERVICE)
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedHost("localhost")
                        .setAdvertisedPort(8080)
                        .build());
    }

    // --- validateBase ---

    @Test
    void validateBase_validRequest_returnsOk() {
        ValidationResult result = RegisterRequestValidator.validateBase(validBaseBuilder().build());
        assertTrue(result.valid());
        assertTrue(result.reasons().isEmpty());
    }

    @Test
    void validateBase_missingName_returnsReason() {
        RegisterRequest request = validBaseBuilder().clearName().build();
        ValidationResult result = RegisterRequestValidator.validateBase(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("name")));
    }

    @Test
    void validateBase_missingConnectivity_returnsReason() {
        RegisterRequest request = RegisterRequest.newBuilder()
                .setName("test-service")
                .setVersion("1.0.0")
                .build();
        ValidationResult result = RegisterRequestValidator.validateBase(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("connectivity")));
    }

    @Test
    void validateBase_emptyHost_returnsReason() {
        RegisterRequest request = validBaseBuilder()
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedPort(8080)
                        .build())
                .build();
        ValidationResult result = RegisterRequestValidator.validateBase(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("advertisedHost")));
    }

    @Test
    void validateBase_zeroPort_returnsReason() {
        RegisterRequest request = validBaseBuilder()
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedHost("localhost")
                        .setAdvertisedPort(0)
                        .build())
                .build();
        ValidationResult result = RegisterRequestValidator.validateBase(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("advertisedPort")));
    }

    @Test
    void validateBase_multipleFailures_collectsAll() {
        RegisterRequest request = RegisterRequest.newBuilder()
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedPort(-1)
                        .build())
                .build();
        ValidationResult result = RegisterRequestValidator.validateBase(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().size() >= 3, "Expected at least 3 reasons: name, host, port");
    }

    // --- validateModuleRequest ---

    @Test
    void validateModuleRequest_validRequest_returnsOk() {
        ValidationResult result = RegisterRequestValidator.validateModuleRequest(validBaseBuilder().build());
        assertTrue(result.valid());
    }

    @Test
    void validateModuleRequest_missingVersion_returnsReason() {
        RegisterRequest request = validBaseBuilder().clearVersion().build();
        ValidationResult result = RegisterRequestValidator.validateModuleRequest(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("version")));
    }

    @Test
    void validateModuleRequest_missingNameAndVersion_collectsBoth() {
        RegisterRequest request = validBaseBuilder().clearName().clearVersion().build();
        ValidationResult result = RegisterRequestValidator.validateModuleRequest(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("name")));
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("version")));
    }

    // --- validateServiceRequest ---

    @Test
    void validateServiceRequest_serviceType_returnsOk() {
        ValidationResult result = RegisterRequestValidator.validateServiceRequest(
                validBaseBuilder().setType(ServiceType.SERVICE_TYPE_SERVICE).build());
        assertTrue(result.valid());
    }

    @Test
    void validateServiceRequest_connectorType_returnsOk() {
        ValidationResult result = RegisterRequestValidator.validateServiceRequest(
                validBaseBuilder().setType(ServiceType.SERVICE_TYPE_CONNECTOR).build());
        assertTrue(result.valid());
    }

    @Test
    void validateServiceRequest_moduleType_returnsReason() {
        ValidationResult result = RegisterRequestValidator.validateServiceRequest(
                validBaseBuilder().setType(ServiceType.SERVICE_TYPE_MODULE).build());
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("type")));
    }

    @Test
    void validateServiceRequest_unspecifiedType_returnsReason() {
        ValidationResult result = RegisterRequestValidator.validateServiceRequest(
                validBaseBuilder().setType(ServiceType.SERVICE_TYPE_UNSPECIFIED).build());
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("type")));
    }

    @Test
    void validateServiceRequest_missingNameAndWrongType_collectsBoth() {
        RegisterRequest request = validBaseBuilder()
                .clearName()
                .setType(ServiceType.SERVICE_TYPE_UNSPECIFIED)
                .build();
        ValidationResult result = RegisterRequestValidator.validateServiceRequest(request);
        assertFalse(result.valid());
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("name")));
        assertTrue(result.reasons().stream().anyMatch(r -> r.contains("type")));
    }

    // --- reasonsAsString ---

    @Test
    void reasonsAsString_formatsCorrectly() {
        RegisterRequest request = RegisterRequest.newBuilder()
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedPort(-1)
                        .build())
                .build();
        ValidationResult result = RegisterRequestValidator.validateBase(request);
        String msg = result.reasonsAsString();
        assertTrue(msg.contains("; "), "Multiple reasons should be semicolon-separated");
    }

    @Test
    void ok_hasEmptyReasons() {
        ValidationResult result = ValidationResult.ok();
        assertTrue(result.valid());
        assertEquals("", result.reasonsAsString());
    }
}
