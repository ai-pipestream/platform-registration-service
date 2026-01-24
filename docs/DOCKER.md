# Docker Build and Push Guide

## Prerequisites
- Docker installed and running
- (Optional) Access to [GitHub Container Registry (GHCR)](https://ghcr.io) with an access token that has `write:packages`

## Build Docker Image

The service uses Quarkus `container-image-docker` extension to build Docker images using the generated Dockerfile.

### Build Image Locally
```bash
./gradlew build -Dquarkus.container-image.build=true
```

This will:
1. Compile the application
2. Create the `build/quarkus-app/` build output
3. Build a Docker image using `src/main/docker/Dockerfile.jvm`
4. Tag the image as:
   - `ghcr.io/ai-pipestream/platform-registration-service:latest`
   - `ghcr.io/ai-pipestream/platform-registration-service:{version}` (where version is the current project version)

### Build and Push to Registry
```bash
./gradlew build \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=true \
  -Dquarkus.container-image.registry=ghcr.io \
  -Dquarkus.container-image.group=ai-pipestream \
  -Dquarkus.container-image.name=platform-registration-service
```

## Configuration

Container image settings are in `src/main/resources/application.properties`:

```properties
quarkus.container-image.registry=ghcr.io
quarkus.container-image.group=ai-pipestream
quarkus.container-image.name=platform-registration-service
quarkus.container-image.tag=latest
quarkus.container-image.additional-tags=${quarkus.application.version}
```

## Available Dockerfiles

Quarkus provides multiple Dockerfiles in `src/main/docker/`:

- **Dockerfile.jvm** - Standard JVM mode (default, used by build)
- **Dockerfile.legacy-jar** - Uber-jar mode
- **Dockerfile.native** - Native compilation (requires GraalVM)
- **Dockerfile.native-micro** - Native with minimal base image

## Running the Container

```bash
docker pull ghcr.io/ai-pipestream/platform-registration-service:latest
docker run -p 38101:8080 \
  -e CONSUL_HOST=consul \
  -e CONSUL_PORT=8500 \
  -e QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres:5432/platform_registration \
  -e QUARKUS_DATASOURCE_USERNAME=registration_user \
  -e QUARKUS_DATASOURCE_PASSWORD=password \
  ghcr.io/ai-pipestream/platform-registration-service:latest
```

## Verify Image

```bash
# List images
docker images | grep platform-registration-service

# Inspect image
docker inspect ghcr.io/ai-pipestream/platform-registration-service:latest
```

## CI/CD Integration

In CI/CD pipelines, use:

```bash
./gradlew clean build \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=true \
  -Dquarkus.container-image.username=$REGISTRY_USER \
  -Dquarkus.container-image.password=$REGISTRY_PASSWORD
```

