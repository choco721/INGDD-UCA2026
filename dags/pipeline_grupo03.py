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
export DBT_TARGET_PATH=/tmp/dbt_target_grupo03
export DBT_LOG_PATH=/tmp/dbt_logs_grupo03
"""

# Intervalo de la corrida, tomado del contexto de Airflow (nunca del reloj).
# En un backfill, estos valores son los del periodo pasado que se reprocesa.
INTERVALO_ISO = (
    '--inicio "{{ data_interval_start.isoformat() }}" '
    '--fin "{{ data_interval_end.isoformat() }}"'
)

# Dias a reconstruir en la serie diaria: del dia en que empieza el intervalo
# al dia en que termina, en hora de Argentina. Incluir el dia de inicio
# reprocesa fotos tomadas despues de la corrida anterior (ej. una ingesta
# manual). Es idempotente: delete+insert por fecha.
VARS_DBT = (
    "--vars '{"
    "\"desde\": \"{{ data_interval_start.in_timezone('America/Argentina/Cordoba').strftime('%Y-%m-%d') }}\", "
    "\"hasta\": \"{{ data_interval_end.in_timezone('America/Argentina/Cordoba').strftime('%Y-%m-%d') }}\""
    "}'"
)

default_args = {
    "owner": "grupo03",
    # Dos reintentos espaciados cubren un corte de la API o de MinIO de unos
    # minutos. Mas que eso esconderia un problema real. Es seguro reintentar
    # porque cada tarea es idempotente.
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

with DAG(
    dag_id="pipeline_grupo03",
    description="Pipeline VATSIM: bronze -> silver -> gold -> PostgreSQL",
    # Primera ingesta real: antes de esta fecha no hay datos que reprocesar.
    start_date=pendulum.datetime(2026, 10, 22, tz="America/Argentina/Cordoba"),
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
python -m scripts.ingesta """ + INTERVALO_ISO + "\n",
    )

    construir = BashOperator(
        task_id="dbt_run",
        bash_command=PREPARAR_ENTORNO + PREPARAR_DBT + f"""
echo "--- Construccion de los modelos silver y gold ---"
dbt run --project-dir {RAIZ}/dbt --profiles-dir {RAIZ}/dbt """ + VARS_DBT + "\n",
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
