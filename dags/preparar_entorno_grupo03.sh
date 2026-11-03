#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUNTIME="$BASE/.runtime_grupo03"
VENV="$RUNTIME/venv"
REQUISITOS="$BASE/requirements_grupo03.txt"
MARCADOR="$RUNTIME/instalacion_completa.txt"

mkdir -p "$RUNTIME"

# Impedir instalaciones simultaneas
exec 9>"$RUNTIME/instalacion.lock"
flock 9

PYTHON_BASE="$(command -v python3)"
VERSION="$("$PYTHON_BASE" --version 2>&1)"
HASH="$(sha256sum "$REQUISITOS" | cut -d' ' -f1)"
ESTADO="$HASH|$VERSION"

# Reutilizar el entorno si sigue funcionando
if [ -f "$MARCADOR" ] && [ -x "$VENV/bin/python" ]; then
    if [ "$(cat "$MARCADOR")" = "$ESTADO" ]; then
        if "$VENV/bin/python" -c \
          "import deltalake, pyarrow, duckdb, dbt, pandas, requests, sqlalchemy, psycopg2" \
          >/dev/null 2>&1; then

            echo "ENTORNO GRUPO03: YA PREPARADO"
            exit 0
        fi
    fi
fi

echo "=== INSTALANDO ENTORNO GRUPO03 ==="
echo "Python base: $VERSION"

# Reconstruir solo el entorno virtual propio
rm -f "$MARCADOR"
rm -rf "$VENV"

"$PYTHON_BASE" -m venv "$VENV"

"$VENV/bin/python" -m pip install \
    --no-cache-dir \
    -r "$REQUISITOS"

"$VENV/bin/python" -m pip check

"$VENV/bin/python" -c \
  "import deltalake, pyarrow, duckdb, dbt, pandas, requests, sqlalchemy, psycopg2"

"$VENV/bin/dbt" --version

printf '%s\n' "$ESTADO" > "$MARCADOR"

echo "ENTORNO GRUPO03: LISTO"
