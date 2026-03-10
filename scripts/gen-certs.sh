#!/usr/bin/env bash
# Generate a self-signed TLS certificate for local/home-server use.
# Run once before `docker compose up`.
#
# Usage:
#   bash scripts/gen-certs.sh
#   bash scripts/gen-certs.sh myserver.local   # custom CN/SAN

set -euo pipefail

CN="${1:-localhost}"
CERT_DIR="$(cd "$(dirname "$0")/.." && pwd)/certs"

mkdir -p "$CERT_DIR"

openssl req -x509 \
  -newkey rsa:4096 \
  -keyout "$CERT_DIR/key.pem" \
  -out    "$CERT_DIR/cert.pem" \
  -days   3650 \
  -nodes \
  -subj   "/CN=$CN" \
  -addext "subjectAltName=DNS:$CN,DNS:localhost,IP:127.0.0.1"

echo "Certificate written to:"
echo "  $CERT_DIR/cert.pem"
echo "  $CERT_DIR/key.pem"
