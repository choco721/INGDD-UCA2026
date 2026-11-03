"""
Configuración del entorno de la materia — Ingeniería de Datos, LCD UCA 2026.

El MISMO material funciona en el Docker local y en el servidor de la cátedra.
Lee todo de variables de entorno; si no están (caso local), usa los valores
por defecto del stack local. No hay que tocar nada al cambiar de entorno.

Orden de resolución de cada valor:
    1) variable de entorno ya presente (la que inyecta JupyterHub por grupo)
    2) archivo .env junto a este entorno.py (lo deja el aprovisionamiento del
       servidor; en local normalmente no existe)
    3) valor por defecto del stack local

Uso en un notebook:
    from entorno import (BUCKET, BRONZE, SILVER, STORAGE_OPTIONS,
                         PG, pg_url, SEED, DBT_DIR, DAGS_DIR, GROUP, info)
    info()   # imprime dónde está parado
"""
import os

# ---- Raíz del material: la carpeta donde vive este entorno.py ----
# Se puede forzar con CURSO_DIR; si no, es el directorio de este archivo.
BASE = os.environ.get("CURSO_DIR") or os.path.dirname(os.path.abspath(__file__))


def _cargar_dotenv(base):
    """Carga BASE/.env en os.environ SIN pisar variables ya definidas."""
    ruta = os.path.join(base, ".env")
    if not os.path.exists(ruta):
        return
    for linea in open(ruta, encoding="utf-8"):
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        os.environ.setdefault(clave.strip(), valor.strip())


_cargar_dotenv(BASE)

# ---- Identidad del grupo ----
# En el servidor: 'grupoNN' (viene por env o .env). En local: 'local'.
GROUP = os.environ.get("GRUPO") or os.environ.get("MINIO_BUCKET") or "local"

# ---- Dónde vive el material (rutas relativas a BASE) ----
SEED     = os.environ.get("SEED_PATH", os.path.join(BASE, "seed", "bcra_series_historico.csv"))
DBT_DIR  = os.environ.get("DBT_DIR",   os.path.join(BASE, "dbt"))
DAGS_DIR = os.environ.get("DAGS_DIR",  os.path.join(BASE, "dags"))

# ---- Object storage (MinIO / S3) ----
BUCKET = os.environ.get("MINIO_BUCKET", "lake")        # servidor: grupoNN · local: lake
BRONZE = f"s3://{BUCKET}/bronze/series"
SILVER = f"s3://{BUCKET}/silver"
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID":     os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL":      os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP":        "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# ---- Data warehouse (PostgreSQL, capa gold) ----
PG = {
    "host":     os.environ.get("PGHOST", "postgres"),
    "port":     os.environ.get("PGPORT", "5432"),
    "dbname":   os.environ.get("PGDATABASE", "gold"),    # servidor: warehouse · local: gold
    "user":     os.environ.get("PGUSER", "airflow"),     # servidor: grupoNN · local: airflow
    "password": os.environ.get("PGPASSWORD", "airflow"),
}


def pg_url():
    return (f"postgresql+psycopg2://{PG['user']}:{PG['password']}"
            f"@{PG['host']}:{PG['port']}/{PG['dbname']}")


def info():
    entorno = "servidor de la cátedra" if os.environ.get("MINIO_BUCKET") else "Docker local"
    print(f"Entorno detectado: {entorno}  (grupo: {GROUP})")
    print(f"  Lake / bucket   : {BUCKET}   →   bronze en {BRONZE}")
    print(f"  PostgreSQL      : base '{PG['dbname']}', usuario '{PG['user']}', host '{PG['host']}'")
    print(f"  Material en     : {BASE}")
