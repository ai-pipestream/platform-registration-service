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
 * Updated to match the new 'VerbNoun' naming convention.
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

    @Override
    public Multi<RegisterServiceResponse> registerService(RegisterServiceRequest request) {
        LOG.infof("Received service registration request for: %s", request.getServiceName());
        // WRAPPER: The proto now expects a RegisterServiceResponse, not a raw RegistrationEvent
        return serviceRegistrationHandler.registerService(request)
                .map(event -> RegisterServiceResponse.newBuilder().setEvent(event).build());
    }

    @Blocking
    @Override
    public Multi<RegisterModuleResponse> registerModule(RegisterModuleRequest request) {
        LOG.infof("Received module registration request for: %s", request.getModuleName());
        // WRAPPER: The proto now expects a RegisterModuleResponse
        return moduleRegistrationHandler.registerModule(request)
                .map(event -> RegisterModuleResponse.newBuilder().setEvent(event).build());
    }

    @Override
    public Uni<UnregisterServiceResponse> unregisterService(UnregisterServiceRequest request) {
        LOG.infof("Received service unregistration request for: %s", request.getServiceName());
        return serviceRegistrationHandler.unregisterService(request);
    }

    @Override
    public Uni<UnregisterModuleResponse> unregisterModule(UnregisterModuleRequest request) {
        LOG.infof("Received module unregistration request for: %s", request.getModuleName());
        return moduleRegistrationHandler.unregisterModule(request);
    }

    @Override
    public Uni<ListServicesResponse> listServices(ListServicesRequest request) {
        // RENAMED: Empty -> ListServicesRequest
        // RENAMED: ServiceListResponse -> ListServicesResponse
        LOG.debug("Received request to list all services");
        return discoveryHandler.listServices();
    }

    @Override
    public Uni<ListModulesResponse> listModules(ListModulesRequest request) {
        // RENAMED: ModuleListResponse -> ListModulesResponse
        LOG.debug("Received request to list all modules");
        return discoveryHandler.listModules();
    }

    @Override
    public Uni<GetServiceResponse> getService(GetServiceRequest request) {
        // HANDLE ONEOF: The proto now uses a 'oneof' for ID vs Name
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
        // RENAMED: ServiceResolveResponse -> ResolveServiceResponse
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
        // RENAMED: ModuleSchemaResponse -> GetModuleSchemaResponse
        LOG.infof("Getting schema for: %s", request.getModuleName());
        return schemaRetrievalHandler.getModuleSchema(request);
    }

    @Override
    public Uni<GetModuleSchemaVersionsResponse> getModuleSchemaVersions(GetModuleSchemaVersionsRequest request) {
        // NEW METHOD in v1 proto
        LOG.infof("Listing schema versions for: %s", request.getModuleName());
        return schemaRetrievalHandler.getModuleSchemaVersions(request);
    }
}