# Platform Registration Service - Registration Flow Design

## Overview

The Piplestream AI Platform is language agnoistic, so the quarkus features that enable gRPC services to be dynamically discovered and registered with Consul are not available.  Instead, we use a gRPC gateway to route requests based on the `ServiceType` field.  This document describes the registration flow for both services and modules.

Given the the module API is 100% grpc and language ignostic, the platform also requires a way to register these services with Consul and register module schemas with Apicurio Registry.

However, forcing developers to create integrations with Consul and Apicurio Registry is not ideal.  Instead, we want to provide a unified API for registering both services and modules with the platform.  This way, a developer only needs to create 2 functions to create a full pipeline without any additional integrations.   Everything is pure gRPC and the platform handles the rest.

The Platform Registration Service provides a **unified API** for registering both services and modules with the platform. The API uses a single `Register` RPC that routes internally based on the `ServiceType` field to differentiate between services and modules.  The `Register` RPC returns a stream of `RegisterResponse` events that provide real-time progress and error messages..

## Proto API

```protobuf
service PlatformRegistrationService {
  // Unified registration - routes based on type field
  rpc Register(RegisterRequest) returns (stream RegisterResponse);
  
  // Unified unregistration
  rpc Unregister(UnregisterRequest) returns (UnregisterResponse);
}

message RegisterRequest {
  string name = 1;           // Service or module name
  ServiceType type = 2;      // SERVICE_TYPE_SERVICE or SERVICE_TYPE_MODULE
  Connectivity connectivity = 3;
  string version = 4;
  map<string, string> metadata = 5;
  repeated string tags = 6;
  repeated string capabilities = 7;
}

message Connectivity {
  string advertised_host = 1;
  int32 advertised_port = 2;
  optional string internal_host = 3;
  optional int32 internal_port = 4;
  bool tls_enabled = 5;
}

message UnregisterRequest {
  string name = 1;
  string host = 2;
  int32 port = 3;
}
```

## High-Level Architecture

```mermaid
flowchart TB
    subgraph Clients
        SVC[Platform Services]
        MOD[Processing Modules]
    end
    
    subgraph PlatformRegistrationService
        GW[gRPC Gateway<br/>PlatformRegistrationService.java]
        SRH[ServiceRegistrationHandler]
        MRH[ModuleRegistrationHandler]
    end
    
    subgraph External Systems
        CONSUL[(Consul)]
        DB[(MySQL)]
        APICURIO[(Apicurio Registry)]
        KAFKA[Kafka]
    end
    
    SVC -->|Register<br/>type=SERVICE| GW
    MOD -->|Register<br/>type=MODULE| GW
    
    GW -->|type=SERVICE| SRH
    GW -->|type=MODULE| MRH
    
    SRH --> CONSUL
    MRH --> CONSUL
    MRH -->|Callback| MOD
    MRH --> DB
    MRH --> APICURIO
    
    SRH --> KAFKA
    MRH --> KAFKA
```

## Request Routing

The `PlatformRegistrationService` gRPC service receives all registration requests and routes them based on the `type` field:

```mermaid
flowchart TD
    A[Register RPC called] --> B{Check request.type}
    B -->|SERVICE_TYPE_SERVICE| C[ServiceRegistrationHandler.registerService]
    B -->|SERVICE_TYPE_MODULE| D[ModuleRegistrationHandler.registerModule]
    B -->|UNSPECIFIED| E[Return Error]
    
    C --> F[Stream RegisterResponse events]
    D --> F
```

## Service Registration Flow

Services are platform components that don't require schema management (e.g., `mapping-service`, `account-service`).

```mermaid
sequenceDiagram
    participant Client as Service Client
    participant PRS as PlatformRegistrationService
    participant SRH as ServiceRegistrationHandler
    participant Consul
    participant Kafka as Kafka (OpenSearch Events)
    
    Client->>PRS: Register(RegisterRequest)
    Note over Client,PRS: type=SERVICE_TYPE_SERVICE
    
    PRS->>SRH: registerService(request)
    
    SRH-->>Client: EVENT_TYPE_STARTED
    
    SRH->>SRH: Validate request
    SRH-->>Client: EVENT_TYPE_VALIDATED
    
    SRH->>Consul: Register service + health check
    SRH-->>Client: EVENT_TYPE_CONSUL_REGISTERED
    SRH-->>Client: EVENT_TYPE_HEALTH_CHECK_CONFIGURED
    
    SRH->>Consul: Wait for healthy
    alt Service becomes healthy
        SRH-->>Client: EVENT_TYPE_CONSUL_HEALTHY
        SRH->>Kafka: Emit ServiceRegistered event
        SRH-->>Client: EVENT_TYPE_COMPLETED
    else Health check timeout
        SRH->>Consul: Deregister (rollback)
        SRH-->>Client: EVENT_TYPE_FAILED
    end
```

### Service Registration Events

| Event | Description |
|-------|-------------|
| `EVENT_TYPE_STARTED` | Registration process initiated |
| `EVENT_TYPE_VALIDATED` | Request validation passed |
| `EVENT_TYPE_CONSUL_REGISTERED` | Successfully registered with Consul |
| `EVENT_TYPE_HEALTH_CHECK_CONFIGURED` | gRPC health check configured |
| `EVENT_TYPE_CONSUL_HEALTHY` | Service passed Consul health checks |
| `EVENT_TYPE_COMPLETED` | Registration complete |
| `EVENT_TYPE_FAILED` | Registration failed (with error_detail) |

## Module Registration Flow

Modules are document processing components that implement `PipeStepProcessorService` and require schema management via Apicurio Registry.

```mermaid
sequenceDiagram
    participant Client as Module Client
    participant PRS as PlatformRegistrationService
    participant MRH as ModuleRegistrationHandler
    participant Consul
    participant Module as Module (callback)
    participant DB as MySQL
    participant Apicurio as Apicurio Registry
    participant Kafka as Kafka (OpenSearch Events)
    
    Client->>PRS: Register(RegisterRequest)
    Note over Client,PRS: type=SERVICE_TYPE_MODULE
    
    PRS->>MRH: registerModule(request)
    
    MRH-->>Client: EVENT_TYPE_STARTED
    
    MRH->>MRH: Validate request
    MRH-->>Client: EVENT_TYPE_VALIDATED
    
    MRH->>Consul: Register service + health check
    MRH-->>Client: EVENT_TYPE_CONSUL_REGISTERED
    MRH-->>Client: EVENT_TYPE_HEALTH_CHECK_CONFIGURED
    
    MRH->>Consul: Wait for healthy
    alt Module becomes healthy
        MRH-->>Client: EVENT_TYPE_CONSUL_HEALTHY
        
        MRH->>Module: GetServiceRegistration()
        Module-->>MRH: Metadata + Schema
        MRH-->>Client: EVENT_TYPE_METADATA_RETRIEVED
        
        MRH->>MRH: Validate/synthesize schema
        MRH-->>Client: EVENT_TYPE_SCHEMA_VALIDATED
        
        MRH->>DB: Save module registration
        MRH-->>Client: EVENT_TYPE_DATABASE_SAVED
        
        MRH->>Apicurio: Register schema
        MRH-->>Client: EVENT_TYPE_APICURIO_REGISTERED
        
        MRH->>Kafka: Emit ModuleRegistered event
        MRH-->>Client: EVENT_TYPE_COMPLETED
    else Health check timeout
        MRH->>Consul: Deregister (rollback)
        MRH-->>Client: EVENT_TYPE_FAILED
    end
```

### Module Registration Events

| Event | Description |
|-------|-------------|
| `EVENT_TYPE_STARTED` | Registration process initiated |
| `EVENT_TYPE_VALIDATED` | Request validation passed |
| `EVENT_TYPE_CONSUL_REGISTERED` | Successfully registered with Consul |
| `EVENT_TYPE_HEALTH_CHECK_CONFIGURED` | gRPC health check configured |
| `EVENT_TYPE_CONSUL_HEALTHY` | Module passed Consul health checks |
| `EVENT_TYPE_METADATA_RETRIEVED` | Module metadata fetched via callback |
| `EVENT_TYPE_SCHEMA_VALIDATED` | JSON schema validated or synthesized |
| `EVENT_TYPE_DATABASE_SAVED` | Registration persisted to MySQL |
| `EVENT_TYPE_APICURIO_REGISTERED` | Schema registered in Apicurio |
| `EVENT_TYPE_COMPLETED` | Registration complete |
| `EVENT_TYPE_FAILED` | Registration failed (with error_detail) |

## Module Callback Flow

When a module registers, the platform makes a callback to fetch metadata:

```mermaid
flowchart LR
    subgraph Platform Registration Service
        MRH[ModuleRegistrationHandler]
        GC[GrpcClientFactory]
    end
    
    subgraph Module Instance
        PSP[PipeStepProcessorService]
    end
    
    MRH -->|1. Get client for 'module-name'| GC
    GC -->|2. Discover via Consul| MRH
    MRH -->|3. GetServiceRegistration| PSP
    PSP -->|4. Return metadata + schema| MRH
```

The callback retrieves:
- `module_name` - Unique identifier
- `version` - Semantic version  
- `json_config_schema` - JSON Schema for configuration validation
- `display_name` - Human-friendly name
- `description` - Module purpose
- `tags` - Categorization tags
- `dependencies` - External dependencies
- `metadata` - Custom key-value pairs

## Unregistration Flow

Unregistration is simpler and unified for both services and modules:

```mermaid
sequenceDiagram
    participant Client
    participant PRS as PlatformRegistrationService
    participant Handler as Registration Handler
    participant Consul
    participant Kafka
    
    Client->>PRS: Unregister(UnregisterRequest)
    Note over Client,PRS: name + host + port
    
    PRS->>Handler: unregister(request)
    Handler->>Handler: Generate serviceId
    Handler->>Consul: Deregister service
    
    alt Success
        Handler->>Kafka: Emit Unregistered event
        Handler-->>Client: success=true
    else Failure
        Handler-->>Client: success=false, message
    end
```

## Data Flow Summary

```mermaid
flowchart TB
    subgraph Input
        REQ[RegisterRequest]
    end
    
    subgraph Extraction
        NAME[name]
        TYPE[type]
        CONN[connectivity]
        VER[version]
        META[metadata]
        TAGS[tags]
        CAPS[capabilities]
    end
    
    subgraph Derived
        SID[serviceId<br/>name-host-port]
        HOST[advertised_host]
        PORT[advertised_port]
    end
    
    subgraph Storage
        CONSUL[(Consul<br/>Service Discovery)]
        DB[(MySQL<br/>Module Metadata)]
        APICURIO[(Apicurio<br/>Schema Registry)]
    end
    
    REQ --> NAME & TYPE & CONN & VER & META & TAGS & CAPS
    NAME --> SID
    CONN --> HOST & PORT
    HOST & PORT --> SID
    
    NAME & HOST & PORT & VER & TAGS & CAPS --> CONSUL
    NAME & HOST & PORT & VER & META --> DB
    NAME & VER --> APICURIO
```

## Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| `PlatformRegistrationService` | gRPC gateway, routes by type |
| `ServiceRegistrationHandler` | Service registration logic |
| `ModuleRegistrationHandler` | Module registration logic with callback |
| `ConsulRegistrar` | Consul service registration |
| `ConsulHealthChecker` | Wait for service health |
| `ModuleRepository` | MySQL persistence |
| `ApicurioRegistryClient` | Schema registry operations |
| `RegistrationGrpcClients` | Dynamic gRPC client for callbacks |
| `OpenSearchEventsProducer` | Kafka event emission |

## Key Design Decisions

1. **Unified API**: Single `Register` method routes by `type`, not separate methods per type
2. **Streaming Response**: Real-time progress via streaming events
3. **Module Callback**: Platform fetches metadata from module rather than client providing it
4. **Schema Management**: Only modules have schemas; managed via Apicurio Registry
5. **Connectivity Abstraction**: `Connectivity` message allows advertised vs internal addresses
6. **Natural Key**: Services identified by `name + host + port` tuple

