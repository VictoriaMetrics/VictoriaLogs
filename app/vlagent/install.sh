#!/bin/sh
set -e

VERSION=v1.50.0

UNAME="$(uname -s)-$(uname -m)"

# See https://en.wikipedia.org/wiki/Uname#Examples
if   [ "$UNAME" = "Linux-x86_64"   ]; then PLATFORM="linux-amd64"
elif [ "$UNAME" = "Linux-aarch64"  ]; then PLATFORM="linux-arm64"
elif [ "$UNAME" = "Linux-i686"     ]; then PLATFORM="linux-386"
elif [ "$UNAME" = "Darwin-x86_64"  ]; then PLATFORM="darwin-amd64"
elif [ "$UNAME" = "Darwin-arm64"   ]; then PLATFORM="darwin-arm64"
elif [ "$UNAME" = "FreeBSD-x86_64" ]; then PLATFORM="freebsd-amd64"
elif [ "$UNAME" = "OpenBSD-x86_64" ]; then PLATFORM="openbsd-amd64"
else
  echo "Unsupported platform: $UNAME"
  echo "Try to install vlagent from source code: https://docs.victoriametrics.com/victorialogs/vlagent/#building-from-source-code"
  exit 1
fi

URL="https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/${VERSION}/vlutils-${PLATFORM}-${VERSION}.tar.gz"

echo "Downloading vlutils-${PLATFORM}-${VERSION}.tar.gz..."
curl -fsSL "$URL" | tar -xz vlagent-prod

echo "vlagent $VERSION installed, use ./vlagent-prod to run"
