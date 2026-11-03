"""Publica la capa gold en PostgreSQL para su consumo desde Metabase.

Lee las tablas de marts construidas por dbt en DuckDB y las escribe en el
esquema del grupo dentro de la base de serving. La estrategia es de reemplazo
total en cada corrida: la capa gold se reconstruye entera, de modo que el
tablero siempre refleja el ultimo estado del pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

from entorno import DBT_DIR, pg_url

ESQUEMA_POSTGRES = "grupo03"

# Tablas de marts a publicar. El nombre es el mismo en DuckDB y en PostgreSQL.
TABLAS = [
    "fct_trafico_snapshot",
    "fct_rutas",
    "fct_aeronaves",
    "fct_posiciones",
]


def ruta_duckdb() -> Path:
    """Devuelve la ruta de la base DuckDB construida por dbt.

    Returns:
        Ruta al archivo warehouse.duckdb.

    Raises:
        FileNotFoundError: Si la base no existe todavia.
    """
    ruta = Path(
        os.getenv("DBT_DUCKDB_PATH", str(Path(DBT_DIR) / "warehouse.duckdb"))
    )
    if not ruta.is_file():
        raise FileNotFoundError(f"No existe la base DuckDB: {ruta}")
    return ruta


def leer_gold() -> dict[str, pd.DataFrame]:
    """Lee todas las tablas de marts desde DuckDB.

    Returns:
        Diccionario con el nombre de cada tabla y su contenido.

    Raises:
        ValueError: Si alguna tabla esperada esta vacia.
    """
    con = duckdb.connect(str(ruta_duckdb()), read_only=True)
    try:
        datos: dict[str, pd.DataFrame] = {}
        for tabla in TABLAS:
            df = con.execute(f"select * from main.{tabla}").df()
            if df.empty:
                raise ValueError(f"La tabla gold '{tabla}' esta vacia.")
            datos[tabla] = df
            print(f"  {tabla}: {len(df)} filas leidas")
        return datos
    finally:
        con.close()


def publicar_resultado(datos: dict[str, pd.DataFrame]) -> int:
    """Publica las tablas de marts en PostgreSQL.

    Verifica que la conexion apunte a la base y el usuario del grupo antes de
    escribir, y comprueba que la cantidad publicada coincida con la leida.

    Args:
        datos: Tablas a publicar, por nombre.

    Returns:
        Total de filas publicadas.

    Raises:
        RuntimeError: Si la conexion no es la esperada o si el conteo no coincide.
    """
    motor = create_engine(pg_url())
    total_general = 0

    try:
        with motor.begin() as con:
            base = con.execute(text("select current_database()")).scalar_one()
            usuario = con.execute(text("select current_user")).scalar_one()

            if base != "warehouse" or usuario != ESQUEMA_POSTGRES:
                raise RuntimeError(
                    f"Conexion PostgreSQL inesperada: {base}, {usuario}"
                )

            existe = con.execute(
                text(
                    "select 1 from information_schema.schemata "
                    "where schema_name = :esquema"
                ),
                {"esquema": ESQUEMA_POSTGRES},
            ).scalar()
            if not existe:
                raise RuntimeError(
                    f"No existe el esquema '{ESQUEMA_POSTGRES}' en PostgreSQL "
                    "y el usuario no tiene permiso para crearlo."
                )

            for tabla, df in datos.items():
                df.to_sql(
                    name=tabla,
                    con=con,
                    schema=ESQUEMA_POSTGRES,
                    if_exists="replace",
                    index=False,
                    chunksize=500,
                )
                publicadas = con.execute(
                    text(f"select count(*) from {ESQUEMA_POSTGRES}.{tabla}")
                ).scalar_one()

                if publicadas != len(df):
                    raise RuntimeError(
                        f"'{tabla}': se leyeron {len(df)} filas y se "
                        f"publicaron {publicadas}."
                    )

                print(f"  {tabla}: {publicadas} filas publicadas")
                total_general += publicadas

        return total_general

    finally:
        motor.dispose()


def publicar_gold() -> int:
    """Ejecuta la publicacion completa de la capa gold.

    Returns:
        Total de filas publicadas en PostgreSQL.
    """
    print("=== PUBLICACION GOLD VATSIM ===")
    print("Lectura desde DuckDB:")
    datos = leer_gold()

    print("Escritura en PostgreSQL:")
    total = publicar_resultado(datos)

    print(f"Destino: warehouse.{ESQUEMA_POSTGRES}")
    print(f"Total de filas publicadas: {total}")
    return total


if __name__ == "__main__":
    publicar_gold()
