"""Ingesta incremental e idempotente de VATSIM hacia la capa bronze.

VATSIM publica una sola foto del momento (se renueva cada ~15 s) con todos los
pilotos conectados, sin historico ni filtro por fecha. Cada corrida trae esa
foto y la agrega a bronze solo si es nueva.

Uso:
    python -m scripts.ingesta                       # ingesta a demanda
    python -m scripts.ingesta --inicio ISO --fin ISO # corrida de Airflow

Codigos de salida: 0 ok o sin novedades · 1 falla transitoria · 2 falla
permanente.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
import requests
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from entorno import BUCKET, STORAGE_OPTIONS

BRONZE = f"s3://{BUCKET}/bronze/vatsim"
BASE_URL = "https://data.vatsim.net/v3/vatsim-data.json"
CLAVE = ["cid", "snapshot_ts"]

# Reintentos dentro del script: cubren cortes de segundos. Los cortes de
# minutos los cubren los reintentos de Airflow (default_args del DAG).
ESPERAS_REINTENTO = [5, 15]
TIMEOUT = 30
ESTADOS_TRANSITORIOS = {429, 500, 502, 503, 504}

# Margen para aceptar una foto como parte del intervalo de la corrida: la
# corrida programada se ejecuta al final de su intervalo, mas preparacion y
# reintentos.
TOLERANCIA = pd.Timedelta(hours=3)


class FallaTransitoria(Exception):
    """Error que puede resolverse solo: reintentar tiene sentido."""


class FallaPermanente(Exception):
    """Error que no cambia al reintentar: hay que corregir algo."""


def validar_configuracion() -> None:
    """Corta antes de empezar si falta una variable obligatoria."""
    requeridas = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                  "AWS_ENDPOINT_URL", "MINIO_BUCKET"]
    faltantes = [v for v in requeridas if not os.getenv(v)]
    if faltantes:
        raise FallaPermanente("Faltan variables: " + ", ".join(faltantes))


def leer_watermark(destino: str = BRONZE) -> pd.Timestamp | None:
    """Devuelve el snapshot mas reciente que ya esta en bronze.

    El watermark sale de los datos ingeridos, no de un archivo aparte, asi
    que no puede desincronizarse de ellos.

    Args:
        destino: Ruta de la tabla Delta de bronze.

    Returns:
        El ultimo snapshot_ts, o None si bronze todavia no existe.
    """
    try:
        dt = DeltaTable(destino, storage_options=STORAGE_OPTIONS)
    except TableNotFoundError:
        return None
    ts = dt.to_pandas(columns=["snapshot_ts"])["snapshot_ts"].dropna()
    return None if ts.empty else pd.to_datetime(ts, utc=True).max()


def pedir_a_api() -> dict:
    """Pide la foto actual a VATSIM, con reintentos solo para fallas transitorias.

    Returns:
        El JSON de la respuesta, ya validado como objeto.

    Raises:
        FallaTransitoria: Timeout, sin conexion, 429 o 5xx tras agotar reintentos.
        FallaPermanente: Otro codigo HTTP o un cuerpo que no es JSON.
    """
    for intento, espera in enumerate([0, *ESPERAS_REINTENTO], start=1):
        time.sleep(espera)
        try:
            r = requests.get(BASE_URL, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as exc:
            motivo = type(exc).__name__
        else:
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError as exc:
                    raise FallaPermanente("La respuesta no es JSON valido") from exc
                if not isinstance(data, dict):
                    raise FallaPermanente("El JSON no tiene la forma esperada")
                return data
            if r.status_code not in ESTADOS_TRANSITORIOS:
                raise FallaPermanente(f"VATSIM respondio HTTP {r.status_code}")
            motivo = f"HTTP {r.status_code}"
        print(f"Intento {intento} fallido ({motivo}).")
    raise FallaTransitoria(f"VATSIM no respondio tras {intento} intentos ({motivo})")


def validar_respuesta(data: dict) -> tuple[pd.Timestamp, str, list[dict]]:
    """Comprueba que la respuesta traiga lo que necesitamos antes de usarla.

    Args:
        data: JSON devuelto por la API.

    Returns:
        (snapshot como fecha, snapshot como texto original, lista de pilotos).

    Raises:
        FallaPermanente: Si falta la marca de tiempo, no se puede leer, o no
            hay pilotos. Una foto vacia no se escribe: casi seguro es un error
            de la fuente, no una red sin trafico.
    """
    try:
        texto_ts = data["general"]["update_timestamp"]
        pilotos = data["pilots"]
    except (KeyError, TypeError) as exc:
        raise FallaPermanente(f"Respuesta sin el campo esperado: {exc}") from exc
    try:
        snapshot = pd.Timestamp(texto_ts)
    except (ValueError, TypeError) as exc:
        raise FallaPermanente(f"update_timestamp ilegible: {texto_ts!r}") from exc
    if not isinstance(pilotos, list) or not pilotos:
        raise FallaPermanente("La respuesta no trae pilotos")
    faltan = {"cid", "latitude", "longitude"} - set(pilotos[0])
    if faltan:
        raise FallaPermanente(f"A los pilotos les faltan campos: {sorted(faltan)}")
    return snapshot, texto_ts, pilotos


def ingerir(inicio: pd.Timestamp | None = None,
            fin: pd.Timestamp | None = None,
            destino: str = BRONZE) -> int:
    """Trae la foto actual y la agrega a bronze si es nueva.

    Args:
        inicio: Comienzo del intervalo de la corrida (lo pasa Airflow).
        fin: Fin del intervalo de la corrida (lo pasa Airflow).
        destino: Ruta de la tabla Delta de bronze.

    Returns:
        Cantidad de filas escritas.
    """
    watermark = leer_watermark(destino)

    # Si bronze ya tiene una foto posterior al intervalo pedido, la API solo
    # puede devolver algo igual o mas nuevo: no hace falta ni preguntarle.
    # Evita una llamada a VATSIM por cada dia de un backfill largo.
    if fin is not None and watermark is not None and watermark > fin + TOLERANCIA:
        print(f"Intervalo [{inicio}, {fin}] anterior al watermark ({watermark}): "
              "VATSIM no guarda historico, nada que ingerir.")
        return 0

    snapshot, texto_ts, pilotos = validar_respuesta(pedir_a_api())

    # La fuente solo tiene el presente. Si Airflow pide un intervalo pasado
    # (backfill), la foto actual no le pertenece: no hay nada que ingerir.
    # La decision sale de comparar la foto con el intervalo, no con el reloj.
    if inicio is not None and fin is not None:
        if not (inicio - TOLERANCIA <= snapshot <= fin + TOLERANCIA):
            print(f"La foto disponible ({texto_ts}) no pertenece al intervalo "
                  f"[{inicio}, {fin}]. VATSIM no guarda historico: nada que "
                  "ingerir. La transformacion de este intervalo sale de bronze.")
            return 0

    # Barrera 1 — watermark: no se escribe lo que ya se tiene.
    if watermark is not None and snapshot <= watermark:
        print(f"Sin snapshot nuevo (watermark={watermark}).")
        return 0

    df = pd.json_normalize(pilotos, sep="_")
    df["snapshot_ts"] = texto_ts  # bronze guarda el valor crudo, tal cual vino
    df = df.astype("string").drop_duplicates(CLAVE)

    # Barrera 2 — red de seguridad ante dos corridas simultaneas: descarta
    # claves que otra corrida haya escrito para esta misma foto.
    try:
        ya = DeltaTable(destino, storage_options=STORAGE_OPTIONS).to_pandas(
            columns=CLAVE, filters=[("snapshot_ts", "=", texto_ts)])
        claves = set(map(tuple, ya.values))
        df = df[~df[CLAVE].apply(tuple, axis=1).isin(claves)]
    except TableNotFoundError:
        pass
    if df.empty:
        print("Esta foto ya estaba en bronze.")
        return 0

    df = df.assign(_ingested_at=datetime.now(timezone.utc).isoformat(),
                   _run_id=uuid.uuid4().hex)
    # schema_mode="merge": si VATSIM agrega un campo, bronze lo incorpora como
    # columna nueva en vez de fallar. Bronze guarda lo que vino; decidir si el
    # campo nuevo importa es trabajo de silver.
    write_deltalake(destino, df, mode="append", schema_mode="merge",
                    storage_options=STORAGE_OPTIONS)
    print(f"Ingeridas {len(df)} filas (snapshot {texto_ts}, "
          f"watermark previo {watermark}).")
    return len(df)


def main() -> None:
    """Punto de entrada: traduce cada tipo de falla a un codigo de salida."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--inicio", help="inicio del intervalo (ISO 8601)")
    parser.add_argument("--fin", help="fin del intervalo (ISO 8601)")
    args = parser.parse_args()
    inicio = pd.Timestamp(args.inicio) if args.inicio else None
    fin = pd.Timestamp(args.fin) if args.fin else None

    try:
        validar_configuracion()
        filas = ingerir(inicio, fin)
    except FallaTransitoria as exc:
        print(f"FALLA TRANSITORIA: {exc}", file=sys.stderr)
        sys.exit(1)
    except FallaPermanente as exc:
        print(f"FALLA PERMANENTE (reintentar no sirve): {exc}", file=sys.stderr)
        sys.exit(2)
    print("Filas ingeridas:", filas)


if __name__ == "__main__":
    main()
