#!/bin/bash
set -e

# Platform Registration Service - Docker Build Script
#
# This script provides easy commands for building Docker images to different registries
#
# Usage:
#   ./build-docker.sh <registry> [native|multi-arch]
#
# Registry options:
#   local              # Build to local Docker (no push)
#   localhost          # Push to localhost:5000 registry
#   ghcr               # Push to GitHub Container Registry
#   dockerhub          # Push to Docker Hub
#
# Build type options (optional):
#   native             # Build native image (small, fast startup)
#   multi-arch         # Build for AMD64 + ARM64
#
# Examples:
#   ./build-docker.sh local              # Local JVM build
#   ./build-docker.sh local native       # Local native build
#   ./build-docker.sh ghcr multi-arch    # Multi-arch to GHCR
#   ./build-docker.sh localhost native   # Native to localhost:5000

REGISTRY="${1:-local}"
BUILD_TYPE="${2:-jvm}"

# Parse build type
NATIVE=false
MULTIARCH=false

if [[ "$BUILD_TYPE" == "native" ]]; then
  NATIVE=true
elif [[ "$BUILD_TYPE" == "multi-arch" ]]; then
  MULTIARCH=true
elif [[ "$BUILD_TYPE" != "jvm" && -n "$BUILD_TYPE" ]]; then
  echo "Unknown build type: $BUILD_TYPE"
  echo "Valid options: jvm (default), native, multi-arch"
  exit 1
fi

echo "Building platform-registration-service"
echo "  Registry: $REGISTRY"
echo "  Build Type: $BUILD_TYPE"
echo ""

# Validate registry
case "$REGISTRY" in
  local|localhost|ghcr|dockerhub)
    # Valid registry
    ;;
  *)
    echo "Unknown registry: $REGISTRY"
    echo ""
    echo "Available registries:"
    echo "  local      - Build to local Docker (no push)"
    echo "  localhost  - Push to localhost:5000 registry"
    echo "  ghcr       - Push to GitHub Container Registry"
    echo "  dockerhub  - Push to Docker Hub"
    exit 1
    ;;
esac

# Determine registry details
case "$REGISTRY" in
  local)
    REGISTRY_URL=""
    REGISTRY_DISPLAY="local Docker"
    ;;
  localhost)
    REGISTRY_URL="localhost:5000/"
    REGISTRY_DISPLAY="localhost:5000"
    ;;
  ghcr)
    REGISTRY_URL="ghcr.io/"
    REGISTRY_DISPLAY="GitHub Container Registry"
    ;;
  dockerhub)
    REGISTRY_URL="docker.io/"
    REGISTRY_DISPLAY="Docker Hub"
    ;;
esac

# Handle multi-arch builds
if [[ "$MULTIARCH" == "true" ]]; then
  echo "Building multi-architecture image (amd64 + arm64) for $REGISTRY_DISPLAY..."
  echo "Note: This requires Docker Buildx and QEMU"
  echo ""

  # Create buildx builder if it doesn't exist
  if ! docker buildx ls | grep -q multiarch; then
    docker buildx create --name multiarch --use
  else
    docker buildx use multiarch
  fi

  # Build the app first
  if [[ "$NATIVE" == "true" ]]; then
    echo "Error: Multi-arch native builds are not yet supported"
    echo "Please use either multi-arch OR native, not both"
    exit 1
  fi

  ./gradlew clean build -x test

  # Determine push flag
  if [[ "$REGISTRY" == "local" ]]; then
    PUSH_FLAG="--load"
  else
    PUSH_FLAG="--push"
  fi

  docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --sbom=true \
    --provenance=true \
    -f src/main/docker/Dockerfile.jvm \
    -t "${REGISTRY_URL}ai-pipestream/platform-registration-service:latest" \
    $PUSH_FLAG \
    .

  echo ""
  echo "✓ Multi-arch image built for amd64 + arm64"
  echo "  Tagged as: ${REGISTRY_URL}ai-pipestream/platform-registration-service:latest"

# Handle native builds
elif [[ "$NATIVE" == "true" ]]; then
  echo "Building native image for $REGISTRY_DISPLAY..."
  echo "Note: This will take several minutes (5-10 min)"
  echo ""

  ./gradlew clean build -x test \
    -Dquarkus.profile=$REGISTRY \
    -Dquarkus.package.type=native \
    -Dquarkus.container-image.build=true

  echo ""
  echo "✓ Native image built (much smaller size, faster startup)"
  echo "  Tagged as: ${REGISTRY_URL}ai-pipestream/platform-registration-service:latest"

# Handle standard JVM builds
else
  echo "Building JVM image for $REGISTRY_DISPLAY..."
  echo ""

  ./gradlew clean build -x test \
    -Dquarkus.profile=$REGISTRY \
    -Dquarkus.container-image.build=true

  echo ""
  echo "✓ JVM image built"
  echo "  Tagged as: ${REGISTRY_URL}ai-pipestream/platform-registration-service:latest"
fi

echo ""
echo "Build completed successfully!"
