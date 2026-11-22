"""Despliega el pipeline desde el clon del repo a la carpeta que monta Airflow.

El repo es la fuente de verdad: los cambios se hacen ahi, pasan el CI, y
este script los copia a /home/jovyan/work/dags. Asi nadie edita a mano la
carpeta de produccion, que es lo que rompio el pipeline dos veces.

Uso, desde una celda de Jupyter en el servidor:

    !python /home/jovyan/work/INGDD-UCA2026/herramientas/desplegar.py

Antes de pisar nada guarda una copia de lo que habia, para poder volver
atras. Nunca toca .env, el entorno virtual, warehouse.duckdb ni target/.
Al final corre el chequeo previo al despliegue.
"""

from __future__ import annotations

import filecmp
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DESTINO = Path("/home/jovyan/work/dags")
RESPALDOS = Path("/home/jovyan/work/respaldos_despliegue")

# Lo que se despliega, relativo a dags/ en ambos lados.
ARCHIVOS = [
    "pipeline_grupo03.py", "entorno.py", "requirements_grupo03.txt",
    "preparar_entorno_grupo03.sh", ".airflowignore",
    "dbt/dbt_project.yml", "dbt/profiles.yml", "dbt/.dbtignore",
]
CARPETAS = ["scripts", "dbt/models", "dbt/tests", "dbt/ci"]
IGNORAR = {"__pycache__", ".ipynb_checkpoints"}


def a_desplegar() -> list[str]:
    """Lista los archivos del repo que van al servidor, relativos a dags/."""
    origen = REPO / "dags"
    rutas = [a for a in ARCHIVOS if (origen / a).is_file()]
    for carpeta in CARPETAS:
        for p in sorted((origen / carpeta).rglob("*")):
            if p.is_file() and not IGNORAR & set(p.parts):
                rutas.append(str(p.relative_to(origen)))
    return rutas


def main() -> None:
    """Copia los archivos que cambiaron, con respaldo previo, y verifica."""
    marca = datetime.now().strftime("%Y%m%d_%H%M%S")
    respaldo = RESPALDOS / marca
    nuevos, cambiados = [], []

    for rel in a_desplegar():
        src, dst = REPO / "dags" / rel, DESTINO / rel
        if dst.is_file() and filecmp.cmp(src, dst, shallow=False):
            continue
        if dst.is_file():
            (respaldo / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dst, respaldo / rel)
            cambiados.append(rel)
        else:
            nuevos.append(rel)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        dst.chmod(0o644 if not rel.endswith(".sh") else 0o755)

    print(f"Despliegue desde {REPO}\n")
    for rel in cambiados:
        print(f"  actualizado  {rel}")
    for rel in nuevos:
        print(f"  nuevo        {rel}")
    if not (nuevos or cambiados):
        print("  sin cambios: el servidor ya tenia esta version")
    if cambiados:
        print(f"\nRespaldo de lo anterior en {respaldo}")

    print("\nChequeo previo:\n")
    chequeo = REPO / "herramientas" / "verificar_despliegue.py"
    sys.exit(subprocess.run([sys.executable, str(chequeo)]).returncode)


if __name__ == "__main__":
    main()
