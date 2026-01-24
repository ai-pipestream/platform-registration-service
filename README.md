# Platform Registration Service

The Platform Registration Service manages service discovery and health monitoring for the pipeline platform. It keeps track of all running services, modules, and connectors, ensuring they are reachable and healthy.

## Overview

This service maintains a registry of every component in the platform. When a service starts, it registers itself here with its location and capabilities. The registration service then monitors its health, manages its configuration schemas, and notifies other systems about its status through Kafka events.

This service is used by other components in the platform to discover and communicate with each other.  The frontend uses this service as a way to act as a proxy to consul, which is not accessible from the outside world.  The frontend also uses this service to retrieve configuration schemas for modules and connectors.  It's central in the role of the frontend and backend - the engine cannot process without it.

### Architectural Role

This service is the source of truth for service discovery. By centralizing registration, we ensure all components follow the same standards for health checks and metadata.

#### 1. Unified Registration

Instead of each service talking to Consul or Kafka directly, they make a single gRPC call to this service. The registration service then handles:

*   Validating the metadata and configuration schema.
*   Registering the service in Consul with health checks.
*   Broadcasting registration events to Kafka.

This ensures consistency across the platform and simplifies the logic needed in each individual service.

#### 2. Registration Types

The platform supports three distinct types of components:

*   **Services**: Core platform components like the API Gateway or Auth Service. These are registered in Consul for discovery and health monitoring.
*   **Modules**: Specialized processing units (e.g., OCR, NLP). Registration involves a callback where the platform fetches versioned configuration schemas and metadata directly from the module.
*   **Connectors**: Integration points for external systems (e.g., S3, Kafka, PostgreSQL). These are registered for discovery and can also provide schemas for their source/sink configurations.

#### 3. Frontend Sidecar

The `platform-registration-service` also serves as an entry point for the web frontend. The frontend's Node.js backend communicates with this service's gRPC endpoints instead of talking to Consul directly. This provides a clear security boundary and abstracts the underlying discovery mechanism.

### Core Functions

**Service Registration & Discovery**
*   Registry for services, modules, and connectors.
*   Directory of running services with their locations and capabilities.
*   Filtering by tags and capabilities.

**Health Monitoring**
*   Continuous monitoring using gRPC health checks.
*   Automatic removal of unhealthy services.
*   Readiness probes for dependencies.

**Module & Connector Management**
*   Schema validation for processing modules and data connectors.
*   Versioned configuration storage.
*   Integration with the dynamic gRPC client factory.

**Schema Management**
*   Storage: PostgreSQL for metadata and Apicurio Registry for schema versions.
*   JSON Schema validation and versioning.
*   Automatic schema synchronization between the database and Apicurio.
*   Multi-tier schema retrieval (Database → Apicurio → Direct Call).

**Event Streaming**
*   Publishes registration and unregistration events to Kafka.
*   Integrates with OpenSearch for indexing.
*   Uses Protobuf for event serialization.

**Self-Registration**
- Automatically registers itself with Consul on startup
- Health check configuration and validation
- Service discovery integration

## Architecture

### Technology Stack

**Backend**
*   **Framework**: Quarkus with reactive programming (Mutiny)
*   **Database**: PostgreSQL with Hibernate Reactive Panache
*   **Service Discovery**: Consul
*   **Schema Registry**: Apicurio Registry v3
*   **Messaging**: Kafka with SmallRye Reactive Messaging
*   **API**: gRPC with Connect-RPC support

**Frontend**
*   **Framework**: Vue 3 with Vuetify 3
*   **Build Tool**: Vite with Quinoa integration

## Database Schema

We use PostgreSQL to store metadata about registered modules and their configuration schemas.

### Modules Table
```sql
CREATE TABLE modules (
    service_id VARCHAR(255) PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    version VARCHAR(100),
    config_schema_id VARCHAR(255),
    metadata JSONB,
    registered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
);
```

### Config Schemas Table
```sql
CREATE TABLE config_schemas (
    schema_id VARCHAR(255) PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    schema_version VARCHAR(100) NOT NULL,
    json_schema JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    apicurio_artifact_id VARCHAR(255),
    apicurio_global_id BIGINT,
    sync_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    last_sync_attempt TIMESTAMP,
    sync_error VARCHAR(255),
    CONSTRAINT unique_service_schema_version UNIQUE(service_name, schema_version)
);
```

## Building Locally

### Prerequisites
- Java 21 (Temurin, Oracle, or any compatible distribution)
- Docker (optional but required to run the container locally)
- Internet access to Maven Central and GitHub Packages
- GitHub Personal Access Token with `read:packages` (GitHub Packages requires auth even for public artifacts)

### Clone & Build
```bash
git clone https://github.com/ai-pipestream/platform-registration-service.git
cd platform-registration-service

# Configure GitHub Packages credentials (optional, for faster dependency resolution)
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=your-personal-access-token

# Build the application (skip tests for a faster first run)
./gradlew clean build -x test
```

> **Tip:** Remove `-x test` once you have the required infrastructure (PostgreSQL, Kafka, Consul) running locally or via Testcontainers.  This is automatically setup via Quarkus Dev Services through the compose plugin.  If you have docker and jdk21 installed, tests should work.

### Build & Run the Docker Image
You can let Quarkus build the image or build it manually with Docker.

**Quarkus-managed build**
```bash
./gradlew clean build -x test \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=false \
  -Dquarkus.container-image.registry=ghcr.io \
  -Dquarkus.container-image.group=ai-pipestream \
  -Dquarkus.container-image.name=platform-registration-service \
  -Dquarkus.container-image.tag=local

docker run -p 38101:8080 ghcr.io/ai-pipestream/platform-registration-service:local
```

**Manual docker build**
```bash
./gradlew clean quarkusBuild -x test
docker build -f src/main/docker/Dockerfile.jvm -t platform-registration-service:local .
docker run -p 38101:8080 platform-registration-service:local
```

### Troubleshooting
- **401 from GitHub Packages** – double-check `GITHUB_ACTOR`/`GITHUB_TOKEN`.
- **Docker build fails** – ensure `./gradlew quarkusBuild` succeeded so that `build/quarkus-app/` exists.
- **Tests fail** – services like PostgreSQL, Kafka, and Consul must be available; skip tests until you’re ready to configure them.

## Docker Build Process

### Prerequisites
- Docker installed and running
- Java 21+ and Gradle for building the application
- Access to Red Hat UBI9 base images

### Build Steps

**1. Build the Application**
```bash
# From the project root
./gradlew build
```

**2. Build Docker Image**
```bash
# Build the Docker image
docker build -f src/main/docker/Dockerfile.jvm -t platform-registration-service:latest .
```

**3. Alternative Build with Custom Tag**
```bash
docker build -f src/main/docker/Dockerfile.jvm -t your-registry/platform-registration-service:v1.0.0 .
```

### Dockerfile Details

**Base Image**: `registry.access.redhat.com/ubi9/openjdk-21:1.21`

**Multi-layer Structure**:
- **Dependencies Layer**: `/deployments/lib/` - Application JARs and dependencies
- **Application Layer**: `/deployments/*.jar` - Main application JARs
- **Resources Layer**: `/deployments/app/` - Application resources and configuration
- **Runtime Layer**: `/deployments/quarkus/` - Quarkus runtime components

**Security**:
- Runs as non-root user (UID 185)
- Minimal attack surface with UBI9 base
- No unnecessary packages or tools

**Port Configuration**:
- Exposes port 8080 by default
- Configurable via environment variables
- Supports both HTTP and gRPC on same port

### Runtime Configuration

**Environment Variables**:
```bash
# Consul Configuration
CONSUL_HOST=consul
CONSUL_PORT=8500

# JVM Configuration
JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"

# Profile Configuration
QUARKUS_PROFILE=prod

# Database Configuration (if not using service discovery)
QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres:5432/platform_registration
QUARKUS_DATASOURCE_USERNAME=pipeline
QUARKUS_DATASOURCE_PASSWORD=password

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Apicurio Registry Configuration
APICURIO_REGISTRY_URL=http://apicurio:8081/apis/registry/v3
```

**Memory Management**:
- Automatic heap sizing based on container memory limits
- Configurable via `JAVA_MAX_MEM_RATIO` (default: 50%)
- Initial heap sizing via `JAVA_INITIAL_MEM_RATIO` (default: 25%)

**Health Checks**:
```bash
# Readiness check
curl http://localhost:8080/q/health/ready

# Liveness check  
curl http://localhost:8080/q/health/live

# Metrics endpoint
curl http://localhost:8080/q/metrics
```

### Docker Integration Testing

For containerized integration testing, use the centralized `docker-integration-tests` directory in the ai-pipestream monorepo. This provides shared infrastructure and service-specific overlays.

**Prerequisites**:
- Docker and Docker Compose installed
- Access to the ai-pipestream monorepo

**Start infrastructure and service**:
```bash
cd /path/to/ai-pipestream/docker-integration-tests

# Start shared infrastructure (PostgreSQL, Kafka, Consul, Apicurio, etc.)
docker compose up -d

# Start platform-registration-service with infrastructure
docker compose -f docker-compose.yml \
  -f services/platform-registration-service/docker-compose.yml \
  -f services/platform-registration-service/docker-compose.snapshot.yml \
  up -d platform-registration-service

# View logs
docker logs -f platform-registration-service

# Check service health
curl http://localhost:38201/platform-registration/q/health/ready

# Run validation tests
bash services/platform-registration-service/test.sh
```

**Stop services**:
```bash
docker compose -f docker-compose.yml \
  -f services/platform-registration-service/docker-compose.yml \
  down

# Remove volumes (clean slate)
docker compose down -v
```

**Port Mapping**:
- External: `38201` (avoids conflict with Quarkus dev mode on 38101)
- Internal: `38101`
- Context path: `/platform-registration`

**Note**: For local development, use Quarkus Dev Services which automatically manages infrastructure.

## Development

### Local Development Setup

**Prerequisites**:
- Java 21+
- Node.js 22+ and pnpm
- Docker and Docker Compose
- Gradle

**Start Dependencies**:
```bash
# Start infrastructure services using the startup script
./scripts/start-platform-registration.sh
```

**Run in Development Mode**:
```bash
# Start the service in Quarkus dev mode
./gradlew quarkusDev
```

**Frontend Development**:
```bash
# Install dependencies
cd src/main/ui-vue
pnpm install

# Start development server
pnpm run dev
```

### Testing

**Run Tests**:
```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "ai.pipestream.registration.handlers.*Test"
```

**Integration Testing**:
- Uses Testcontainers for PostgreSQL
- Mock Consul and Apicurio for unit tests
- End-to-end tests with real infrastructure

## API Reference

### gRPC Services

**PlatformRegistration Service**:
- `registerService(ServiceRegistrationRequest) -> Multi<RegistrationEvent>`
- `registerModule(ModuleRegistrationRequest) -> Multi<RegistrationEvent>`
- `unregisterService(UnregisterRequest) -> Uni<UnregisterResponse>`
- `unregisterModule(UnregisterRequest) -> Uni<UnregisterResponse>`
- `listServices(Empty) -> Uni<ServiceListResponse>`
- `listModules(Empty) -> Uni<ModuleListResponse>`
- `getService(ServiceLookupRequest) -> Uni<ServiceDetails>`
- `getModule(ServiceLookupRequest) -> Uni<ModuleDetails>`
- `resolveService(ServiceResolveRequest) -> Uni<ServiceResolveResponse>`
- `watchServices(Empty) -> Multi<ServiceListResponse>`
- `watchModules(Empty) -> Multi<ModuleListResponse>`
- `getModuleSchema(GetModuleSchemaRequest) -> Uni<ModuleSchemaResponse>` **[NEW]**

#### getModuleSchema - Centralized Schema Retrieval

The `getModuleSchema` RPC method provides centralized access to module configuration schemas with the following features:

**Request Parameters:**
- `service_name` (required): Name of the module
- `version` (optional): Specific version to retrieve. If omitted, returns the latest version

**Response Fields:**
- `service_name`: Name of the module
- `schema_json`: JSON Schema or OpenAPI schema as string
- `schema_version`: Version of the schema
- `artifact_id`: Apicurio Registry artifact ID (if available)
- `metadata`: Map of additional metadata (owner, description, created_by, etc.)
- `updated_at`: Timestamp of when the schema was last updated

**Retrieval Strategy:**
The service uses a multi-tier approach to retrieve schemas:
1.  **PostgreSQL (Primary)**: Checks the local database for the system of record.
2.  **Apicurio Registry (Secondary)**: Falls back to Apicurio if the schema is not found in the database.
3.  **Direct Call (Tertiary)**: Calls the component's `getServiceRegistration()` endpoint directly.
4.  **Synthesis (Fallback)**: Generates a default OpenAPI 3.1 schema if no other source is available.

**Example Usage (Frontend):**
```typescript
const client = createPlatformRegistrationClient();

// Get the latest schema
const latestResponse = await client.getModuleSchema({
  serviceName: 'pdf-extractor'
});

// Get a specific version
const versionedResponse = await client.getModuleSchema({
  serviceName: 'pdf-extractor',
  version: '2.1.0'
});

const schema = JSON.parse(latestResponse.schemaJson);
console.log('Schema version:', latestResponse.schemaVersion);
console.log('Artifact ID:', latestResponse.artifactId);
console.log('Metadata:', latestResponse.metadata);
```

**Benefits:**
- ✅ Schema available even if module is down
- ✅ Centralized versioning through Apicurio
- ✅ Single service call (no need for dynamic transports)
- ✅ Rich metadata support
- ✅ Full version history
- ✅ Graceful fallback to module direct call

### REST Endpoints

**Health Checks**:
- `GET /q/health/ready` - Readiness probe
- `GET /q/health/live` - Liveness probe
- `GET /q/metrics` - Prometheus metrics

**Management**:
- `GET /swagger-ui` - API documentation
- `GET /platform-registration/` - Web UI

## Monitoring & Observability

### Metrics
- Service registration/unregistration counts
- Health check success/failure rates
- Database connection pool metrics
- Consul client metrics
- Apicurio Registry sync status

### Logging
- Structured logging with JSON format
- File rotation with size limits
- Log levels configurable per package
- Request/response logging for gRPC calls

### Health Checks
- **Dependent Services**: PostgreSQL, Consul, Apicurio Registry
- **Service Health**: gRPC health service integration
- **Readiness**: All dependencies must be healthy
- **Liveness**: Application must be responding

## Configuration

### Application Properties

**Core Configuration**:
```properties
# Service Configuration
quarkus.application.name=platform-registration-service
quarkus.http.port=38101
quarkus.grpc.server.use-separate-server=false

# Database
quarkus.datasource.db-kind=postgresql
quarkus.hibernate-orm.schema-management.strategy=none
quarkus.flyway.migrate-at-start=true
```

## Troubleshooting

If you encounter issues during registration or discovery, check the following areas.

### Common Issues

*   **Registration Failures**: Usually caused by connectivity issues with Consul. Verify that the service can reach Consul and that the gRPC health service is correctly implemented in the registering service.
*   **Database Connectivity**: Ensure PostgreSQL is running and the credentials match what is in `application.properties`. If you see migration errors, check the Flyway status.
*   **Schema Sync**: If schemas aren't appearing in Apicurio, check the `config_schemas` table for sync errors.

### Debugging

To get more detailed information about what's happening internally, you can enable trace logging for specific packages in `application.properties`:

```properties
# Trace platform registration logic
quarkus.log.category."ai.pipestream.registration".level=TRACE

# Debug Consul service discovery
quarkus.log.category."io.vertx.consul".level=DEBUG

# Debug database queries
quarkus.log.category."org.hibernate.SQL".level=DEBUG
```

For remote debugging in a container, run with `JAVA_DEBUG=true`:
```bash
docker run -e JAVA_DEBUG=true -e JAVA_DEBUG_PORT=*:5005 -p 5005:5005 platform-registration-service
```

## Contributing

### Code Style
- Java 21 with Quarkus reactive programming
- 4-space indentation
- Package naming: `ai.pipestream.registration.*`
- Class naming: PascalCase
- Method/field naming: camelCase

### Testing Requirements
- Unit tests for all business logic
- Integration tests for external dependencies
- Frontend tests for Vue components
- End-to-end tests for critical paths

### Pull Request Process
1. Create feature branch from main
2. Implement changes with tests
3. Update documentation if needed
4. Run full test suite
5. Submit PR with clear description

## License

This project is part of the Pipeline Platform and follows the same licensing terms as the main project.

## Build and Deployment

For detailed information about building, versioning, and deploying this service, see [BUILD_DEPLOYMENT.md](BUILD_DEPLOYMENT.md).

### Quick Start

#### Local Development
```bash
# Build and test
./gradlew clean build test

# Run locally in dev mode
./gradlew quarkusDev
```

#### Docker Deployment
```bash
# Run with Docker
docker run -d \
  --name platform-registration-service \
  -p 8080:8080 \
  ghcr.io/ai-pipestream/platform-registration-service:latest
```

### Release Process
1. Ensure changes are merged to main
2. Go to GitHub Actions → **"Release and Publish"**
3. Click "Run workflow" and select version bump type
4. Monitor pipeline completion
5. Verify release in GitHub releases

For complete deployment instructions, see [BUILD_DEPLOYMENT.md](BUILD_DEPLOYMENT.md).
