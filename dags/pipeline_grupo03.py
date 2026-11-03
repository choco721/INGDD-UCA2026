"""DAG del pipeline de datos de VATSIM — grupo03.

Orquesta el pipeline completo: ingesta incremental a la capa bronze,
transformación con dbt (silver y gold), validación con tests y publicación
de la capa gold en PostgreSQL para consumo desde Metabase.

El DAG solo coordina: la lógica vive en scripts/ y en el proyecto dbt/.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Raíz del grupo dentro del contenedor de Airflow. El servidor monta
# /home/jovyan/work/dags (workspace del grupo) en esta ruta.
RAIZ = "/opt/airflow/dags/grupo03"

# Prefijo común a todas las tareas:
#   - se posiciona en la raíz del grupo,
#   - carga el .env (credenciales y parámetros, fuera del código),
#   - expone la raíz en PYTHONPATH para poder importar entorno y scripts.
PREPARAR_ENTORNO = f"""
set -euo pipefail
cd {RAIZ}
set -a
. ./.env
set +a
export PYTHONPATH="{RAIZ}:${{PYTHONPATH:-}}"
export PATH="{RAIZ}/.runtime_grupo03/venv/bin:${{PATH}}"
"""

# Variables que el perfil de dbt espera resolver con env_var().
PREPARAR_DBT = f"""
export DBT_DUCKDB_PATH="{RAIZ}/dbt/warehouse.duckdb"
ENDPOINT="${{AWS_ENDPOINT_URL#http://}}"
export DBT_S3_ENDPOINT="${{ENDPOINT#https://}}"
export DBT_S3_USE_SSL=false
"""

default_args = {
    "owner": "grupo03",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

with DAG(
    dag_id="pipeline_grupo03",
    description="Pipeline VATSIM: bronze -> silver -> gold -> PostgreSQL",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Cordoba"),
    schedule="0 18 * * *",  # diario a las 18:00 (hora del start_date): mantiene
                            # constante la hora de observacion entre dias
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["proyecto-integrador", "vatsim", "grupo03"],
) as dag:

    preparar_entorno = BashOperator(
        task_id="preparar_entorno",
        bash_command=f"bash {RAIZ}/preparar_entorno_grupo03.sh ",  # el espacio final evita que Jinja lo tome como archivo de template
    )

    ingerir = BashOperator(
        task_id="ingerir_bronze",
        bash_command=PREPARAR_ENTORNO + """
echo "--- Ingesta incremental de VATSIM hacia bronze ---"
python -m scripts.ingesta
""",
    )

    construir = BashOperator(
        task_id="dbt_run",
        bash_command=PREPARAR_ENTORNO + PREPARAR_DBT + f"""
echo "--- Construccion de los modelos silver y gold ---"
dbt run --project-dir {RAIZ}/dbt --profiles-dir {RAIZ}/dbt
""",
    )

    validar = BashOperator(
        task_id="dbt_test",
        bash_command=PREPARAR_ENTORNO + PREPARAR_DBT + f"""
echo "--- Tests de calidad sobre silver y gold ---"
dbt test --project-dir {RAIZ}/dbt --profiles-dir {RAIZ}/dbt
""",
    )

    publicar = BashOperator(
        task_id="publicar_gold",
        bash_command=PREPARAR_ENTORNO + f"""
echo "--- Publicacion de la capa gold en PostgreSQL ---"
export DBT_DUCKDB_PATH="{RAIZ}/dbt/warehouse.duckdb"
python -m scripts.publicar_gold
""",
    )

    preparar_entorno >> ingerir >> construir >> validar >> publicar
