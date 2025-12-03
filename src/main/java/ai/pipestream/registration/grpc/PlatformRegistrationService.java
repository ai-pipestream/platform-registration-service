package ai.pipestream.registration.grpc;

import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.platform.registration.v1.MutinyPlatformRegistrationServiceGrpc;
import ai.pipestream.registration.handlers.ServiceRegistrationHandler;
import ai.pipestream.registration.handlers.ModuleRegistrationHandler;
import ai.pipestream.registration.handlers.ServiceDiscoveryHandler;
import ai.pipestream.registration.handlers.SchemaRetrievalHandler;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Main platform registration service implementation.
 * Updated to use the unified Register/Unregister API from the new proto definition.
 */
@GrpcService
public class PlatformRegistrationService extends MutinyPlatformRegistrationServiceGrpc.PlatformRegistrationServiceImplBase {

    private static final Logger LOG = Logger.getLogger(PlatformRegistrationService.class);

    @Inject
    ServiceRegistrationHandler serviceRegistrationHandler;

    @Inject
    ModuleRegistrationHandler moduleRegistrationHandler;

    @Inject
    ServiceDiscoveryHandler discoveryHandler;

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    /**
     * Unified registration method - routes based on ServiceType.
     * Replaces the old separate registerService/registerModule methods.
     */
    @Blocking
    @Override
    public Multi<RegisterResponse> register(RegisterRequest request) {
        ServiceType type = request.getType();
        LOG.infof("Received registration request for: %s (type=%s)", request.getName(), type);

        Multi<RegistrationEvent> events;
        
        switch (type) {
            case SERVICE_TYPE_SERVICE:
                events = serviceRegistrationHandler.registerService(request);
                break;
            case SERVICE_TYPE_MODULE:
                events = moduleRegistrationHandler.registerModule(request);
                break;
            case SERVICE_TYPE_UNSPECIFIED:
            case UNRECOGNIZED:
            default:
                LOG.errorf("Invalid service type: %s", type);
                return Multi.createFrom().failure(
                    new IllegalArgumentException("Must specify service type: SERVICE_TYPE_SERVICE or SERVICE_TYPE_MODULE"));
        }
        
        // Wrap RegistrationEvent in RegisterResponse
        return events.map(event -> RegisterResponse.newBuilder().setEvent(event).build());
    }

    /**
     * Unified unregistration method.
     * Replaces the old separate unregisterService/unregisterModule methods.
     * Routes to appropriate handler based on looking up the service in Consul.
     * For simplicity, we try service unregistration first.
     */
    @Override
    public Uni<UnregisterResponse> unregister(UnregisterRequest request) {
        LOG.infof("Received unregistration request for: %s at %s:%d", 
            request.getName(), request.getHost(), request.getPort());
        
        // Both handlers now use the same ConsulRegistrar.unregisterService method
        // and return the same UnregisterResponse type, so we can use either handler.
        // Using serviceRegistrationHandler as the primary (it's simpler).
        return serviceRegistrationHandler.unregisterService(request);
    }

    @Override
    public Uni<ListServicesResponse> listServices(ListServicesRequest request) {
        LOG.debug("Received request to list all services");
        return discoveryHandler.listServices();
    }

    @Override
    public Uni<ListModulesResponse> listModules(ListModulesRequest request) {
        LOG.debug("Received request to list all modules");
        return discoveryHandler.listModules();
    }

    @Override
    public Uni<GetServiceResponse> getService(GetServiceRequest request) {
        if (request.hasServiceName()) {
            LOG.debugf("Looking up service by name: %s", request.getServiceName());
            return discoveryHandler.getServiceByName(request.getServiceName());
        } else if (request.hasServiceId()) {
            LOG.debugf("Looking up service by ID: %s", request.getServiceId());
            return discoveryHandler.getServiceById(request.getServiceId());
        } else {
            return Uni.createFrom().failure(new IllegalArgumentException("Must provide service_name or service_id"));
        }
    }

    @Override
    public Uni<GetModuleResponse> getModule(GetModuleRequest request) {
        if (request.hasModuleName()) {
            LOG.debugf("Looking up module by name: %s", request.getModuleName());
            return discoveryHandler.getModuleByName(request.getModuleName());
        } else if (request.hasServiceId()) {
            LOG.debugf("Looking up module by ID: %s", request.getServiceId());
            return discoveryHandler.getModuleById(request.getServiceId());
        } else {
            return Uni.createFrom().failure(new IllegalArgumentException("Must provide module_name or service_id"));
        }
    }

    @Override
    public Uni<ResolveServiceResponse> resolveService(ResolveServiceRequest request) {
        LOG.infof("Resolving service: %s (prefer_local=%s)",
                request.getServiceName(), request.getPreferLocal());
        return discoveryHandler.resolveService(request);
    }

    @Override
    public Multi<WatchServicesResponse> watchServices(WatchServicesRequest request) {
        LOG.info("Watching services for updates");
        return discoveryHandler.watchServices();
    }

    @Override
    public Multi<WatchModulesResponse> watchModules(WatchModulesRequest request) {
        LOG.info("Watching modules for updates");
        return discoveryHandler.watchModules();
    }

    @Override
    public Uni<GetModuleSchemaResponse> getModuleSchema(GetModuleSchemaRequest request) {
        LOG.infof("Getting schema for: %s", request.getModuleName());
        return schemaRetrievalHandler.getModuleSchema(request);
    }

    @Override
    public Uni<GetModuleSchemaVersionsResponse> getModuleSchemaVersions(GetModuleSchemaVersionsRequest request) {
        LOG.infof("Listing schema versions for: %s", request.getModuleName());
        return schemaRetrievalHandler.getModuleSchemaVersions(request);
    }
}
