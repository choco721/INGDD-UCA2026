
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import requests
import pandas as pd

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from entorno import BUCKET, STORAGE_OPTIONS


# ============================================
# CONFIGURACION
# ============================================

BRONZE = f"s3://{BUCKET}/bronze/vatsim"

BASE_URL = "https://data.vatsim.net/v3/vatsim-data.json"

CLAVE = ["cid", "snapshot_ts"]


def validar_configuracion():
    """
    Comprueba que existan las variables obligatorias
    antes de ejecutar la ingesta.
    """

    requeridas = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT_URL",
        "MINIO_BUCKET",
    ]

    faltantes = [
        variable
        for variable in requeridas
        if not os.getenv(variable)
    ]

    if faltantes:
        raise RuntimeError(
            "Faltan variables obligatorias: "
            + ", ".join(faltantes)
        )

    print("Configuracion validada correctamente.")


# ============================================
# WATERMARK
# ============================================

def leer_watermark(destino: str = BRONZE) -> str | None:
    """
    Lee el ultimo snapshot almacenado en Bronze.
    """

    try:
        dt = DeltaTable(
            destino,
            storage_options=STORAGE_OPTIONS
        )

    except TableNotFoundError:
        return None

    ts = (
        dt.to_pandas(columns=["snapshot_ts"])
        ["snapshot_ts"]
        .dropna()
    )

    return None if ts.empty else str(ts.max())


# ============================================
# EXTRACCION DESDE VATSIM
# ============================================

def extraer_de_api(desde: str | None = None):
    """
    Consulta la API de VATSIM y devuelve los
    registros de pilotos del ultimo snapshot.
    """

    try:
        r = requests.get(
            BASE_URL,
            timeout=30
        )

        r.raise_for_status()

    except requests.RequestException as exc:

        status = getattr(
            exc.response,
            "status_code",
            None
        )

        raise RuntimeError(
            f"Fallo VATSIM "
            f"({type(exc).__name__}, HTTP {status})"
        ) from None

    data = r.json()

    snapshot_ts = data["general"]["update_timestamp"]

    # Evitar volver a ingerir un snapshot existente
    if desde and snapshot_ts <= desde:

        print(
            f"Sin snapshot nuevo (watermark={desde})."
        )

        return pd.DataFrame()

    # Transformar el JSON en un DataFrame
    df = pd.json_normalize(
        data["pilots"],
        sep="_"
    )

    df["snapshot_ts"] = snapshot_ts

    print(
        f"Filas extraidas: {len(df)} "
        f"(snapshot {snapshot_ts})"
    )

    return df.astype("string")


# ============================================
# INGESTA INCREMENTAL EN BRONZE
# ============================================

def ingerir(extractor, destino: str = BRONZE):
    """
    Extrae nuevos registros y los escribe en
    Bronze evitando duplicados.
    """

    # Consultar el watermark actual
    wm = leer_watermark(destino)

    # Extraer solo datos nuevos
    nuevos = extractor(desde=wm)

    if nuevos is None or nuevos.empty:

        print(
            f"Sin novedades (watermark={wm})."
        )

        return 0

    # Eliminar duplicados dentro de la extraccion
    nuevos = nuevos.drop_duplicates(CLAVE)

    # Evitar registros que ya existen en Bronze
    try:

        existentes = DeltaTable(
            destino,
            storage_options=STORAGE_OPTIONS
        ).to_pandas(columns=CLAVE)

        claves_viejas = set(
            map(tuple, existentes.values)
        )

        nuevos = nuevos[
            ~nuevos[CLAVE]
            .apply(tuple, axis=1)
            .isin(claves_viejas)
        ]

    except TableNotFoundError:
        pass

    if nuevos.empty:

        print(
            "Todo lo extraido ya estaba en bronze "
            f"(watermark={wm})."
        )

        return 0

    # Agregar metadatos de ingesta
    nuevos = nuevos.assign(
        _ingested_at=datetime.now(
            timezone.utc
        ).isoformat(),
        _run_id=uuid.uuid4().hex
    )

    # Escribir en Delta Lake / MinIO
    write_deltalake(
        destino,
        nuevos,
        mode="append",
        storage_options=STORAGE_OPTIONS
    )

    print(
        f"Ingeridas {len(nuevos)} filas "
        f"(watermark previo={wm})."
    )

    return len(nuevos)


# ============================================
# EJECUCION PRINCIPAL
# ============================================

if __name__ == "__main__":

    validar_configuracion()

    filas = ingerir(extraer_de_api)

    print("Filas ingeridas:", filas)
