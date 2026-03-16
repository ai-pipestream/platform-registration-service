package ai.pipestream.registration.handlers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RegistrationVersionSanitizerTest {

    @Test
    void shouldKeepNormalVersion() {
        assertEquals("1.2.3", RegistrationVersionSanitizer.sanitize("1.2.3"));
    }

    @Test
    void shouldFallbackForBlankOrNull() {
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize(""));
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize("  "));
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize(null));
    }

    @Test
    void shouldFallbackForTemplatePlaceholders() {
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize("@version@"));
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize("${project.version}"));
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize("unknown"));
        assertEquals(RegistrationVersionSanitizer.FALLBACK_VERSION, RegistrationVersionSanitizer.sanitize("null"));
    }
}
