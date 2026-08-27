#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME=${1:-go-rpc}
DOCKER_PLATFORM=${2:-linux/amd64}

docker build --platform "$DOCKER_PLATFORM" -f benzhi.Dockerfile -t "$IMAGE_NAME" .

echo
echo "Docker image '$IMAGE_NAME:latest' built successfully for $DOCKER_PLATFORM."
echo "Run: docker run -it $IMAGE_NAME:latest"
