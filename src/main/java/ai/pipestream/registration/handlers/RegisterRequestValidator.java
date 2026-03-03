package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.ServiceType;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared validation logic for {@link RegisterRequest}.
 * Collects all failures rather than short-circuiting on the first one.
 */
public final class RegisterRequestValidator {

    private RegisterRequestValidator() {}

    /**
     * Validates base fields common to all registration requests:
     * name required, connectivity required, host required, port &gt; 0.
     */
    public static ValidationResult validateBase(RegisterRequest request) {
        List<String> reasons = new ArrayList<>();

        if (request.getName().isEmpty()) {
            reasons.add("name is required");
        }
        if (!request.hasConnectivity()) {
            reasons.add("connectivity is required");
        } else {
            if (request.getConnectivity().getAdvertisedHost().isEmpty()) {
                reasons.add("connectivity.advertisedHost is required");
            }
            if (request.getConnectivity().getAdvertisedPort() <= 0) {
                reasons.add("connectivity.advertisedPort must be > 0");
            }
        }

        return reasons.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(reasons);
    }

    /**
     * Validates a module registration request: base checks + version required.
     */
    public static ValidationResult validateModuleRequest(RegisterRequest request) {
        List<String> reasons = new ArrayList<>();

        ValidationResult base = validateBase(request);
        if (!base.valid()) {
            reasons.addAll(base.reasons());
        }

        if (request.getVersion().isEmpty()) {
            reasons.add("version is required for module registration");
        }

        return reasons.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(reasons);
    }

    /**
     * Validates a service registration request: base checks + type must be SERVICE or CONNECTOR.
     */
    public static ValidationResult validateServiceRequest(RegisterRequest request) {
        List<String> reasons = new ArrayList<>();

        ValidationResult base = validateBase(request);
        if (!base.valid()) {
            reasons.addAll(base.reasons());
        }

        ServiceType type = request.getType();
        if (type != ServiceType.SERVICE_TYPE_SERVICE && type != ServiceType.SERVICE_TYPE_CONNECTOR) {
            reasons.add("type must be SERVICE_TYPE_SERVICE or SERVICE_TYPE_CONNECTOR");
        }

        return reasons.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(reasons);
    }
}
