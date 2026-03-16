package ai.pipestream.registration.handlers;

import org.jboss.logging.Logger;

/**
 * Defensive sanitizer for registration version strings coming from modules/services.
 * Prevents unresolved template placeholders from reaching persistence/indexing/registry calls.
 */
public final class RegistrationVersionSanitizer {

    static final String FALLBACK_VERSION = "0.0.0-dev";

    private RegistrationVersionSanitizer() {
    }

    public static String sanitize(String rawVersion, String registrationName, Logger log, String sourceField) {
        String sanitized = sanitize(rawVersion);
        if (!sanitized.equals(rawVersion) && log != null) {
            log.warnf("Resolved invalid version from %s for %s: raw='%s' -> using '%s'",
                    sourceField, registrationName, safe(rawVersion), sanitized);
        }
        return sanitized;
    }

    static String sanitize(String rawVersion) {
        if (rawVersion == null) {
            return FALLBACK_VERSION;
        }

        String trimmed = rawVersion.trim();
        if (trimmed.isEmpty()) {
            return FALLBACK_VERSION;
        }

        if (looksLikePlaceholder(trimmed)) {
            return FALLBACK_VERSION;
        }

        return trimmed;
    }

    private static boolean looksLikePlaceholder(String value) {
        String lower = value.toLowerCase();
        return value.contains("@")
                || (value.contains("${") && value.contains("}"))
                || "unknown".equals(lower)
                || "null".equals(lower);
    }

    private static String safe(String value) {
        if (value == null) {
            return "<null>";
        }
        String trimmed = value.trim();
        if (trimmed.length() <= 120) {
            return trimmed;
        }
        return trimmed.substring(0, 120) + "...";
    }
}
