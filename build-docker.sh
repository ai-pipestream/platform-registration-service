#!/bin/bash
set -e

# Platform Registration Service - Docker Build Script
#
# This script provides easy commands for building Docker images to different registries
#
# Usage:
#   ./build-docker.sh local              # Build to local Docker (no push)
#   ./build-docker.sh localhost          # Push to localhost:5000 registry
#   ./build-docker.sh ghcr               # Push to GitHub Container Registry
#   ./build-docker.sh dockerhub          # Push to Docker Hub
#   ./build-docker.sh native             # Build native image (small, fast startup)
#   ./build-docker.sh multi-arch <profile>  # Build for AMD64 + ARM64

PROFILE="${1:-local}"

echo "Building platform-registration-service with profile: $PROFILE"

case "$PROFILE" in
  local)
    echo "Building to local Docker (no registry push)..."
    ./gradlew clean build -x test \
      -Dquarkus.profile=local \
      -Dquarkus.container-image.build=true
    echo "✓ Image built: ai-pipestream/platform-registration-service:latest"
    ;;

  localhost)
    echo "Building and pushing to localhost:5000..."
    ./gradlew clean build -x test \
      -Dquarkus.profile=localhost \
      -Dquarkus.container-image.build=true
    echo "✓ Image pushed to localhost:5000/ai-pipestream/platform-registration-service:latest"
    ;;

  ghcr)
    echo "Building and pushing to GitHub Container Registry..."
    ./gradlew clean build -x test \
      -Dquarkus.profile=ghcr \
      -Dquarkus.container-image.build=true
    echo "✓ Image pushed to ghcr.io/ai-pipestream/platform-registration-service:latest"
    ;;

  dockerhub)
    echo "Building and pushing to Docker Hub..."
    ./gradlew clean build -x test \
      -Dquarkus.profile=dockerhub \
      -Dquarkus.container-image.build=true
    echo "✓ Image pushed to docker.io/ai-pipestream/platform-registration-service:latest"
    ;;

  native)
    echo "Building native image (this will take several minutes)..."
    ./gradlew clean build -x test \
      -Dquarkus.profile=local \
      -Dquarkus.package.type=native \
      -Dquarkus.container-image.build=true
    echo "✓ Native image built (much smaller size, faster startup)"
    ;;

  multi-arch)
    TARGET_PROFILE="${2:-ghcr}"
    echo "Building multi-architecture image (amd64 + arm64) for $TARGET_PROFILE..."
    echo "Note: This requires Docker Buildx and QEMU"

    # Create buildx builder if it doesn't exist
    if ! docker buildx ls | grep -q multiarch; then
      docker buildx create --name multiarch --use
    else
      docker buildx use multiarch
    fi

    ./gradlew clean build -x test

    case "$TARGET_PROFILE" in
      local)
        REGISTRY=""
        PUSH_FLAG="--load"
        ;;
      localhost)
        REGISTRY="localhost:5000/"
        PUSH_FLAG="--push"
        ;;
      ghcr)
        REGISTRY="ghcr.io/"
        PUSH_FLAG="--push"
        ;;
      dockerhub)
        REGISTRY="docker.io/"
        PUSH_FLAG="--push"
        ;;
      *)
        echo "Unknown profile: $TARGET_PROFILE"
        exit 1
        ;;
    esac

    docker buildx build \
      --platform linux/amd64,linux/arm64 \
      -f src/main/docker/Dockerfile.jvm \
      -t "${REGISTRY}ai-pipestream/platform-registration-service:latest" \
      $PUSH_FLAG \
      .

    echo "✓ Multi-arch image built for amd64 + arm64"
    ;;

  *)
    echo "Unknown profile: $PROFILE"
    echo ""
    echo "Available profiles:"
    echo "  local      - Build to local Docker (no push)"
    echo "  localhost  - Push to localhost:5000 registry"
    echo "  ghcr       - Push to GitHub Container Registry"
    echo "  dockerhub  - Push to Docker Hub"
    echo "  native     - Build native image (smaller, faster)"
    echo "  multi-arch - Build for AMD64 + ARM64"
    exit 1
    ;;
esac

echo ""
echo "Build completed successfully!"
