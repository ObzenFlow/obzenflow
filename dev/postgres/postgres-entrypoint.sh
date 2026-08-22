#!/usr/bin/env bash
set -euo pipefail

tls_dir=/var/lib/postgresql/obzenflow-tls
install -d -o postgres -g postgres -m 0700 "$tls_dir"
install -o postgres -g postgres -m 0644 \
  /run/obzenflow-postgres-tls/server.crt "$tls_dir/server.crt"
install -o postgres -g postgres -m 0600 \
  /run/obzenflow-postgres-tls/server.key "$tls_dir/server.key"

exec /usr/local/bin/docker-entrypoint.sh "$@"
