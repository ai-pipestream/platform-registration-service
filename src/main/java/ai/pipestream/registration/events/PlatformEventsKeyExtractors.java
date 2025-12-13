package ai.pipestream.registration.events;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.platform.registration.v1.ModuleRegistered;
import ai.pipestream.platform.registration.v1.ModuleUnregistered;
import ai.pipestream.platform.registration.v1.ServiceRegistered;
import ai.pipestream.platform.registration.v1.ServiceUnregistered;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

@SuppressWarnings("unused")
public class PlatformEventsKeyExtractors {

    private static UUID extract(String id) {
        if (id == null || id.isEmpty()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(id.getBytes());
        }
    }

    @ApplicationScoped
    public static class ServiceRegisteredExtractor implements UuidKeyExtractor<ServiceRegistered> {
        @Override
        public UUID extractKey(ServiceRegistered event) {
            return extract(event.getServiceId());
        }
    }

    @ApplicationScoped
    public static class ServiceUnregisteredExtractor implements UuidKeyExtractor<ServiceUnregistered> {
        @Override
        public UUID extractKey(ServiceUnregistered event) {
            return extract(event.getServiceId());
        }
    }

    @ApplicationScoped
    public static class ModuleRegisteredExtractor implements UuidKeyExtractor<ModuleRegistered> {
        @Override
        public UUID extractKey(ModuleRegistered event) {
            return extract(event.getServiceId());
        }
    }

    @ApplicationScoped
    public static class ModuleUnregisteredExtractor implements UuidKeyExtractor<ModuleUnregistered> {
        @Override
        public UUID extractKey(ModuleUnregistered event) {
            return extract(event.getServiceId());
        }
    }
}
