package ai.pipestream.registration.entity;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class ServiceModuleCrudTest {

    @Test
    @TestTransaction
    void serviceModule_fullCrud_flow() {
        // Create
        ServiceModule module = ServiceModule.create("orders", "127.0.0.1", 9090);
        module.version = "1.0.0";
        module.metadata = new HashMap<>();
        module.metadata.put("env", "test");
        module.metadata.put("replicas", 1);
        final String id = module.serviceId;

        // Persist (flush so @CreationTimestamp registeredAt is generated)
        module.persistAndFlush();

        // Read back (by id)
        ServiceModule found = ServiceModule.findById(id);
        assertNotNull(found, "Expected module to be found after persist");
        assertEquals(id, found.serviceId);
        assertEquals("orders", found.serviceName);
        assertEquals("127.0.0.1", found.host);
        assertEquals(9090, found.port);
        assertEquals("1.0.0", found.version);
        assertNotNull(found.registeredAt, "registeredAt should be set by DB/CreationTimestamp");
        assertNotNull(found.lastHeartbeat, "lastHeartbeat seeded in factory");
        assertEquals(ServiceStatus.ACTIVE, found.status);
        assertEquals("test", found.metadata.get("env"));
        assertEquals(1, ((Number) found.metadata.get("replicas")).intValue());

        // Update
        found.version = "1.1.0";
        found.status = ServiceStatus.UNHEALTHY;
        found.metadata.put("tier", "dev");
        found.updateHeartbeat();

        // Verify the update
        ServiceModule updated = ServiceModule.findById(id);
        assertNotNull(updated);
        assertEquals("1.1.0", updated.version);
        assertEquals(ServiceStatus.UNHEALTHY, updated.status);
        assertEquals("dev", updated.metadata.get("tier"));
        assertTrue(updated.isHealthy(), "Heartbeat should be fresh after update");

        // Delete
        boolean deleted = ServiceModule.deleteById(id);
        assertTrue(deleted, "Expected deleteById to return true");

        assertNull(ServiceModule.findById(id), "Entity should be gone after delete");
    }
}
