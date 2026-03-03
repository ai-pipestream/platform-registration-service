package ai.pipestream.registration.handlers;

import java.util.List;

/**
 * Result of validating a {@link ai.pipestream.platform.registration.v1.RegisterRequest}.
 */
public record ValidationResult(boolean valid, List<String> reasons) {

    public static ValidationResult ok() {
        return new ValidationResult(true, List.of());
    }

    public static ValidationResult failed(List<String> reasons) {
        return new ValidationResult(false, List.copyOf(reasons));
    }

    public String reasonsAsString() {
        return String.join("; ", reasons);
    }
}
