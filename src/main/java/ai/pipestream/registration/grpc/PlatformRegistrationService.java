package ai.pipestream.registration.grpc;

import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.registration.handlers.ServiceRegistrationHandler;
import ai.pipestream.registration.handlers.ModuleRegistrationHandler;
import ai.pipestream.registration.handlers.ServiceDiscoveryHandler;
import ai.pipestream.registration.handlers.SchemaRetrievalHandler;
import ai.pipestream.registration.handlers.TypeDescriptorHandler;
import ai.pipestream.registration.handlers.TypeMetadataHandler;
import com.google.protobuf.Timestamp;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.function.Consumer;

/**
 * Main platform registration service implementation.
 *
 * <p>Implements the blocking (StreamObserver) gRPC contract and runs every RPC on a Quarkus
 * virtual thread ({@link RunOnVirtualThread}). Handlers are plain blocking code; the few
 * genuinely async dependencies (Consul, Vert.x HTTP) are bridged to blocking on the virtual
 * thread, so no work ever pins the Vert.x event loop and there is no reactive plumbing here.
 */
@GrpcService
@RunOnVirtualThread
public class PlatformRegistrationService extends PlatformRegistrationServiceGrpc.PlatformRegistrationServiceImplBase {

    private static final Logger LOG = Logger.getLogger(PlatformRegistrationService.class);

    /** How often the watch streams poll Consul for changes. */
    private static final long WATCH_POLL_INTERVAL_MS = 2000L;

    @Inject
    ServiceRegistrationHandler serviceRegistrationHandler;

    @Inject
    ModuleRegistrationHandler moduleRegistrationHandler;

    @Inject
    ServiceDiscoveryHandler discoveryHandler;

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    @Inject
    TypeDescriptorHandler typeDescriptorHandler;

    @Inject
    TypeMetadataHandler typeMetadataHandler;

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        ServiceType type = request.getType();
        LOG.infof("Received registration request for: %s (type=%s)", request.getName(), type);

        // Each RegistrationEvent the handler produces is wrapped in a RegisterResponse and streamed.
        Consumer<RegistrationEvent> sink = event ->
            responseObserver.onNext(RegisterResponse.newBuilder().setEvent(event).build());

        switch (type) {
            case SERVICE_TYPE_SERVICE:
                serviceRegistrationHandler.registerService(request, sink);
                break;
            case SERVICE_TYPE_CONNECTOR:
                LOG.infof("Routing connector registration for %s to service handler", request.getName());
                serviceRegistrationHandler.registerService(request, sink);
                break;
            case SERVICE_TYPE_MODULE:
                moduleRegistrationHandler.registerModule(request, sink);
                break;
            case SERVICE_TYPE_UNSPECIFIED:
            case UNRECOGNIZED:
            default:
                LOG.errorf("Invalid service type: %s", type);
                responseObserver.onError(new IllegalArgumentException(
                    "Must specify service type: SERVICE_TYPE_SERVICE, SERVICE_TYPE_MODULE, or SERVICE_TYPE_CONNECTOR"));
                return;
        }

        responseObserver.onCompleted();
    }

    @Override
    public void unregister(UnregisterRequest request, StreamObserver<UnregisterResponse> responseObserver) {
        LOG.infof("Received unregistration request for: %s at %s:%d",
            request.getName(), request.getHost(), request.getPort());
        responseObserver.onNext(serviceRegistrationHandler.unregisterService(request));
        responseObserver.onCompleted();
    }

    @Override
    public void listServices(ListServicesRequest request, StreamObserver<ListServicesResponse> responseObserver) {
        LOG.debug("Received request to list all services");
        responseObserver.onNext(discoveryHandler.listServices());
        responseObserver.onCompleted();
    }

    @Override
    public void listPlatformModules(ListPlatformModulesRequest request, StreamObserver<ListPlatformModulesResponse> responseObserver) {
        LOG.debug("Received request to list all modules");
        responseObserver.onNext(discoveryHandler.listModules());
        responseObserver.onCompleted();
    }

    @Override
    public void getService(GetServiceRequest request, StreamObserver<GetServiceResponse> responseObserver) {
        if (request.hasServiceName()) {
            LOG.debugf("Looking up service by name: %s", request.getServiceName());
            responseObserver.onNext(discoveryHandler.getServiceByName(request.getServiceName()));
        } else if (request.hasServiceId()) {
            LOG.debugf("Looking up service by ID: %s", request.getServiceId());
            responseObserver.onNext(discoveryHandler.getServiceById(request.getServiceId()));
        } else {
            responseObserver.onError(new IllegalArgumentException("Must provide service_name or service_id"));
            return;
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getModule(GetModuleRequest request, StreamObserver<GetModuleResponse> responseObserver) {
        if (request.hasModuleName()) {
            LOG.debugf("Looking up module by name: %s", request.getModuleName());
            responseObserver.onNext(discoveryHandler.getModuleByName(request.getModuleName()));
        } else if (request.hasServiceId()) {
            LOG.debugf("Looking up module by ID: %s", request.getServiceId());
            responseObserver.onNext(discoveryHandler.getModuleById(request.getServiceId()));
        } else {
            responseObserver.onError(new IllegalArgumentException("Must provide module_name or service_id"));
            return;
        }
        responseObserver.onCompleted();
    }

    @Override
    public void resolveService(ResolveServiceRequest request, StreamObserver<ResolveServiceResponse> responseObserver) {
        LOG.infof("Resolving service: %s (prefer_local=%s)",
                request.getServiceName(), request.getPreferLocal());
        responseObserver.onNext(discoveryHandler.resolveService(request));
        responseObserver.onCompleted();
    }

    @Override
    public void watchServices(WatchServicesRequest request, StreamObserver<WatchServicesResponse> responseObserver) {
        LOG.info("Watching services for updates");
        ServerCallStreamObserver<WatchServicesResponse> observer = serverObserver(responseObserver);

        // Send initial list immediately
        WatchServicesResponse initial = toWatchServicesResponse(discoveryHandler.listServices());
        LOG.infof("Sending initial service list with %d services", initial.getTotalCount());
        responseObserver.onNext(initial);

        // Then poll for changes until the client cancels/disconnects
        try {
            while (!observer.isCancelled()) {
                Thread.sleep(WATCH_POLL_INTERVAL_MS);
                if (observer.isCancelled()) {
                    break;
                }
                try {
                    WatchServicesResponse update = toWatchServicesResponse(discoveryHandler.listServices());
                    LOG.debugf("Service watch update: %d services", update.getTotalCount());
                    responseObserver.onNext(update);
                } catch (Exception e) {
                    LOG.error("Error during service watch", e);
                    responseObserver.onNext(buildEmptyWatchServicesResponse());
                }
            }
            LOG.info("Service watch stream cancelled by client");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Service watch stream interrupted");
        }
    }

    @Override
    public void watchModules(WatchModulesRequest request, StreamObserver<WatchModulesResponse> responseObserver) {
        LOG.info("Watching modules for updates");
        ServerCallStreamObserver<WatchModulesResponse> observer = serverObserver(responseObserver);

        // Send initial list immediately
        WatchModulesResponse initial = toWatchModulesResponse(discoveryHandler.listModules());
        LOG.infof("Sending initial module list with %d modules", initial.getTotalCount());
        responseObserver.onNext(initial);

        // Then poll for changes until the client cancels/disconnects
        try {
            while (!observer.isCancelled()) {
                Thread.sleep(WATCH_POLL_INTERVAL_MS);
                if (observer.isCancelled()) {
                    break;
                }
                try {
                    WatchModulesResponse update = toWatchModulesResponse(discoveryHandler.listModules());
                    LOG.debugf("Module watch update: %d modules", update.getTotalCount());
                    responseObserver.onNext(update);
                } catch (Exception e) {
                    LOG.error("Error during module watch", e);
                    responseObserver.onNext(buildEmptyWatchModulesResponse());
                }
            }
            LOG.info("Module watch stream cancelled by client");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Module watch stream interrupted");
        }
    }

    @Override
    public void getModuleSchema(GetModuleSchemaRequest request, StreamObserver<GetModuleSchemaResponse> responseObserver) {
        LOG.infof("Getting schema for: %s", request.getModuleName());
        responseObserver.onNext(schemaRetrievalHandler.getModuleSchema(request));
        responseObserver.onCompleted();
    }

    @Override
    public void getSchemaArtifact(ai.pipestream.platform.registration.v1.GetSchemaArtifactRequest request,
                                  StreamObserver<ai.pipestream.platform.registration.v1.GetSchemaArtifactResponse> responseObserver) {
        LOG.infof("Getting schema artifact: %s", request.getName());
        responseObserver.onNext(schemaRetrievalHandler.getSchemaArtifact(request));
        responseObserver.onCompleted();
    }

    @Override
    public void getModuleSchemaVersions(GetModuleSchemaVersionsRequest request, StreamObserver<GetModuleSchemaVersionsResponse> responseObserver) {
        LOG.infof("Listing schema versions for: %s", request.getModuleName());
        responseObserver.onNext(schemaRetrievalHandler.getModuleSchemaVersions(request));
        responseObserver.onCompleted();
    }

    @Override
    public void getTypeDescriptor(GetTypeDescriptorRequest request,
                                  StreamObserver<GetTypeDescriptorResponse> responseObserver) {
        LOG.infof("Resolving type descriptor for: %s", request.getTypeUrl());
        responseObserver.onNext(typeDescriptorHandler.getTypeDescriptor(request));
        responseObserver.onCompleted();
    }

    @Override
    public void listTypeDescriptors(ListTypeDescriptorsRequest request,
                                    StreamObserver<ListTypeDescriptorsResponse> responseObserver) {
        responseObserver.onNext(typeDescriptorHandler.listTypeDescriptors(request));
        responseObserver.onCompleted();
    }

    @Override
    public void getTypeMetadata(GetTypeMetadataRequest request,
                                StreamObserver<GetTypeMetadataResponse> responseObserver) {
        responseObserver.onNext(typeMetadataHandler.getTypeMetadata(request));
        responseObserver.onCompleted();
    }

    @Override
    public void saveTypeMetadata(SaveTypeMetadataRequest request,
                                 StreamObserver<SaveTypeMetadataResponse> responseObserver) {
        LOG.infof("Saving type metadata: %s scope=%s fields=%d",
                request.getMessageFullName(), request.getScope(), request.getFieldsCount());
        responseObserver.onNext(typeMetadataHandler.saveTypeMetadata(request));
        responseObserver.onCompleted();
    }

    @Override
    public void promoteTypeMetadata(PromoteTypeMetadataRequest request,
                                    StreamObserver<PromoteTypeMetadataResponse> responseObserver) {
        LOG.infof("Promoting type metadata: %s", request.getMessageFullName());
        responseObserver.onNext(typeMetadataHandler.promoteTypeMetadata(request));
        responseObserver.onCompleted();
    }

    private static <T> ServerCallStreamObserver<T> serverObserver(StreamObserver<T> observer) {
        if (observer instanceof ServerCallStreamObserver<T> serverObserver) {
            return serverObserver;
        }
        throw new IllegalStateException(
            "Expected a ServerCallStreamObserver for the watch stream but got "
                + observer.getClass().getName() + "; cannot detect client cancellation");
    }

    private WatchServicesResponse toWatchServicesResponse(ListServicesResponse listResponse) {
        return WatchServicesResponse.newBuilder()
            .addAllServices(listResponse.getServicesList())
            .setAsOf(listResponse.getAsOf())
            .setTotalCount(listResponse.getTotalCount())
            .build();
    }

    private WatchModulesResponse toWatchModulesResponse(ListPlatformModulesResponse listResponse) {
        return WatchModulesResponse.newBuilder()
            .addAllModules(listResponse.getModulesList())
            .setAsOf(listResponse.getAsOf())
            .setTotalCount(listResponse.getTotalCount())
            .build();
    }

    private WatchServicesResponse buildEmptyWatchServicesResponse() {
        return WatchServicesResponse.newBuilder()
            .setAsOf(createTimestamp())
            .setTotalCount(0)
            .build();
    }

    private WatchModulesResponse buildEmptyWatchModulesResponse() {
        return WatchModulesResponse.newBuilder()
            .setAsOf(createTimestamp())
            .setTotalCount(0)
            .build();
    }

    private Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }
}
