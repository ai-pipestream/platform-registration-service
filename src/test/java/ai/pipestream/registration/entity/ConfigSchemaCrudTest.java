package ai.pipestream.registration.entity;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class ConfigSchemaCrudTest {

    @Test
    @TestTransaction
    void configSchema_fullCrud_flow() {
        // Create
        String json = """
        {
          "type":"object",
          "properties":{
            "port":{"type":"integer"},
            "host":{"type":"string"}
          },
          "required":["port","host"]
        }
        """;

        ConfigSchema schema = ConfigSchema.create("orders", "1.0.0", json);
        schema.createdBy = "test";
        final String id = schema.schemaId;

        // Persist (flush so @CreationTimestamp createdAt is generated)
        schema.persistAndFlush();

        // Read
        ConfigSchema found = ConfigSchema.findById(id);
        assertNotNull(found, "Expected to find schema after persist");
        assertEquals(id, found.schemaId);
        assertEquals("orders", found.serviceName);
        assertEquals("1.0.0", found.schemaVersion);
        assertNotNull(found.jsonSchema);
        assertTrue(found.jsonSchema.contains("\"port\""));
        assertTrue(found.jsonSchema.contains("\"host\""));
        assertEquals(ConfigSchema.SyncStatus.PENDING, found.syncStatus);
        assertNotNull(found.createdAt);

        // Update: mark synced
        found.markSynced("artifact-1", 100L);

        ConfigSchema synced = ConfigSchema.findById(id);
        assertEquals(ConfigSchema.SyncStatus.SYNCED, synced.syncStatus);
        assertEquals("artifact-1", synced.apicurioArtifactId);
        assertEquals(100L, synced.apicurioGlobalId);
        assertNotNull(synced.lastSyncAttempt);
        assertNull(synced.syncError);

        // Update: OUT_OF_SYNC then FAILED
        synced.syncStatus = ConfigSchema.SyncStatus.OUT_OF_SYNC;

        ConfigSchema outOfSync = ConfigSchema.findById(id);
        assertEquals(ConfigSchema.SyncStatus.OUT_OF_SYNC, outOfSync.syncStatus);

        outOfSync.markSyncFailed("network error");

        ConfigSchema failed = ConfigSchema.findById(id);
        assertEquals(ConfigSchema.SyncStatus.FAILED, failed.syncStatus);
        assertEquals("network error", failed.syncError);
        assertNotNull(failed.lastSyncAttempt);

        // Delete
        boolean deleted = ConfigSchema.deleteById(id);
        assertTrue(deleted);

        assertNull(ConfigSchema.findById(id), "Entity should be gone after delete");
    }
}
