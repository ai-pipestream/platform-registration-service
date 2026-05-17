package ai.pipestream.registration.topics;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure logic of the module-work topic provisioner: the name contract
 * (pipestream.module.&lt;moduleName&gt;) and the idempotent
 * skip-if-exists decision. No Kafka, no Quarkus — cannot hang.
 * (JUnit Jupiter assertions — platform-reg has no AssertJ on classpath.)
 */
class ModuleTopicProvisionerTest {

    @Test
    void moduleTopic_isPipestreamModulePrefixedName() {
        assertEquals("pipestream.module.echo", ModuleTopicProvisioner.moduleTopic("echo"),
                "module-work topic contract is pipestream.module.<moduleName>");
        assertEquals("pipestream.module.chunker", ModuleTopicProvisioner.moduleTopic("chunker"));
    }

    @Test
    void moduleTopic_blankModuleName_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ModuleTopicProvisioner.moduleTopic("  "),
                "blank module name must fail loud, never a guessed/empty topic");
        assertThrows(IllegalArgumentException.class,
                () -> ModuleTopicProvisioner.moduleTopic(null));
    }

    @Test
    void shouldCreate_falseWhenTopicAlreadyExists_idempotent() {
        assertFalse(ModuleTopicProvisioner.shouldCreate(
                        Set.of("pipestream.module.echo", "other"), "pipestream.module.echo"),
                "existing topic must not be recreated (idempotent)");
    }

    @Test
    void shouldCreate_trueWhenTopicAbsent() {
        assertTrue(ModuleTopicProvisioner.shouldCreate(
                        Set.of("unrelated"), "pipestream.module.echo"),
                "absent topic must be created");
    }
}
