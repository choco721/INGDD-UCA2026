"""Chequeo previo al despliegue del pipeline del grupo03.

Correr despues de tocar cualquier archivo dentro de dags/, y antes de la
proxima corrida programada de Airflow:

    !python /home/jovyan/work/verificar_despliegue.py

Hace lo que haria un CI antes de integrar un cambio: limpia los artefactos
que deja Jupyter y que Airflow o dbt leerian como codigo, asegura que ambos
los ignoren, revisa los permisos que ya rompieron el pipeline, y verifica que
el DAG compile y que el proyecto dbt parsee. No escribe en la base.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

RAIZ = Path("/home/jovyan/work")
DAGS = RAIZ / "dags"
DBT = DAGS / "dbt"
DAG = DAGS / "pipeline_grupo03.py"

# Carpetas que no hace falta recorrer: son pesadas o se regeneran solas.
NO_RECORRER = {".runtime_grupo03", "target", "logs", ".git", "__pycache__"}
CHECKPOINTS = ".ipynb_checkpoints"

resultados: list[tuple[str, str]] = []


def informar(estado: str, mensaje: str) -> None:
    """Registra e imprime el resultado de un control.

    Args:
        estado: OK, ARREGLADO o FALLA.
        mensaje: Descripcion breve del control.
    """
    resultados.append((estado, mensaje))
    print(f"  [{estado:9s}] {mensaje}")


def limpiar_checkpoints() -> None:
    """Borra las carpetas de checkpoints de Jupyter dentro de dags/."""
    borrados = []
    for actual, dirs, _ in os.walk(DAGS):
        if CHECKPOINTS in dirs:
            dirs.remove(CHECKPOINTS)
            ruta = Path(actual) / CHECKPOINTS
            shutil.rmtree(ruta)
            borrados.append(ruta.relative_to(DAGS))
        dirs[:] = [d for d in dirs if d not in NO_RECORRER]

    if borrados:
        detalle = ", ".join(str(b) for b in borrados)
        informar("ARREGLADO", f"checkpoints de Jupyter borrados: {detalle}")
    else:
        informar("OK", "sin checkpoints de Jupyter dentro de dags/")


def asegurar_ignore(archivo: Path, herramienta: str) -> None:
    """Garantiza que un archivo de exclusiones ignore los checkpoints.

    Args:
        archivo: .airflowignore o .dbtignore.
        herramienta: Nombre para el mensaje.
    """
    texto = archivo.read_text(encoding="utf-8") if archivo.is_file() else ""
    if CHECKPOINTS in texto:
        informar("OK", f"{herramienta} ignora {CHECKPOINTS}")
        return
    archivo.write_text(texto.rstrip() + f"\n{CHECKPOINTS}\n"
                       if texto else f"{CHECKPOINTS}\n", encoding="utf-8")
    informar("ARREGLADO", f"{herramienta}: se agrego {CHECKPOINTS} a {archivo.name}")


def revisar_permisos() -> None:
    """Revisa que Airflow, que corre con otro usuario, pueda leer y escribir."""
    env = DAGS / ".env"
    if env.is_file() and not env.stat().st_mode & stat.S_IROTH:
        informar("FALLA", ".env no es legible para Airflow (chmod 644)")
    else:
        informar("OK", ".env legible para Airflow")

    for nombre in ("warehouse.duckdb", "warehouse.duckdb.wal"):
        ruta = DBT / nombre
        if not ruta.is_file():
            continue
        modo = ruta.stat().st_mode
        if modo & stat.S_IROTH and modo & stat.S_IWOTH:
            informar("OK", f"{nombre} escribible para Airflow")
        else:
            ruta.chmod(modo | stat.S_IROTH | stat.S_IWOTH)
            informar("ARREGLADO", f"{nombre}: permisos de escritura para Airflow")


def verificar_dag() -> None:
    """Compila el DAG en memoria, sin dejar archivos .pyc en dags/."""
    try:
        compile(DAG.read_text(encoding="utf-8"), str(DAG), "exec")
        informar("OK", "el DAG compila")
    except SyntaxError as error:
        informar("FALLA", f"el DAG no compila: linea {error.lineno}: {error.msg}")


def configurar_entorno_dbt() -> dict[str, str]:
    """Arma las variables que el profiles.yml resuelve con env_var().

    Returns:
        Entorno para el proceso de dbt.
    """
    sys.path.insert(0, str(RAIZ))
    from entorno import BUCKET, STORAGE_OPTIONS  # noqa: PLC0415

    url = urlsplit(STORAGE_OPTIONS["AWS_ENDPOINT_URL"])
    env = dict(os.environ)
    env.update({
        "AWS_ACCESS_KEY_ID": STORAGE_OPTIONS["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": STORAGE_OPTIONS["AWS_SECRET_ACCESS_KEY"],
        "AWS_EC2_METADATA_DISABLED": "true",
        "MINIO_BUCKET": BUCKET,
        "DBT_S3_ENDPOINT": url.netloc,
        "DBT_S3_USE_SSL": str(url.scheme == "https").lower(),
        "DBT_DUCKDB_PATH": str(DBT / "warehouse.duckdb"),
        # Fuera de dags/: asi el chequeo no deja archivos de jovyan
        # que despues Airflow no pueda sobrescribir.
        "DBT_TARGET_PATH": "/tmp/dbt_target_chequeo",
        "DBT_LOG_PATH": "/tmp/dbt_logs_chequeo",
    })
    return env


def verificar_dbt() -> None:
    """Corre dbt parse: detecta YAML roto, modelos duplicados y refs invalidas."""
    r = subprocess.run(
        ["dbt", "parse", "--profiles-dir", "."],
        cwd=DBT, env=configurar_entorno_dbt(),
        capture_output=True, text=True,
    )
    if r.returncode == 0:
        informar("OK", "dbt parsea el proyecto")
    else:
        informar("FALLA", "dbt parse falla:\n" + r.stdout[-1500:])


def main() -> None:
    """Ejecuta todos los controles y resume el resultado."""
    print("Chequeo previo al despliegue — pipeline_grupo03\n")
    limpiar_checkpoints()
    asegurar_ignore(DAGS / ".airflowignore", "Airflow")
    asegurar_ignore(DBT / ".dbtignore", "dbt")
    revisar_permisos()
    verificar_dag()
    verificar_dbt()

    fallas = [m for e, m in resultados if e == "FALLA"]
    print("\n" + "=" * 58)
    if fallas:
        print("NO LISTO: corregir lo marcado como FALLA antes de la proxima corrida.")
        sys.exit(1)
    print("LISTO: la proxima corrida programada no deberia tropezar con esto.")


if __name__ == "__main__":
    main()
