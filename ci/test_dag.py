"""Verifica el DAG sin ejecutarlo. Lo corre el CI en cada push.

Controla que los DAGs importen sin errores, que pipeline_grupo03 tenga las
cinco tareas encadenadas en orden y reintentos configurados, y que los
templates de Jinja rendericen con un intervalo pasado: un error de template
no aparece al importar, aparece recien cuando Airflow corre la tarea.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.models import DagBag

ORDEN = ["preparar_entorno", "ingerir_bronze", "dbt_run", "dbt_test",
         "publicar_gold"]


def main() -> None:
    """Corre los controles y sale con codigo 1 si alguno falla."""
    bag = DagBag(dag_folder="dags", include_examples=False)
    errores: list[str] = [f"importacion de {f}: {e}"
                          for f, e in bag.import_errors.items()]

    dag = bag.dags.get("pipeline_grupo03")  # sin consultar la base de Airflow
    if dag is None:
        errores.append("no se encontro el DAG pipeline_grupo03")
    else:
        if [t for t in ORDEN if t not in dag.task_ids]:
            errores.append(f"faltan tareas; se esperaban {ORDEN}")
        else:
            for antes, despues in zip(ORDEN, ORDEN[1:]):
                if despues not in dag.get_task(antes).downstream_task_ids:
                    errores.append(f"{despues} no depende de {antes}")
        if not dag.default_args.get("retries"):
            errores.append("el DAG no tiene reintentos configurados")

        # Un intervalo pasado, como el de un backfill.
        inicio = pendulum.datetime(2026, 11, 9, 21, tz="UTC")
        fin = inicio.add(days=1)
        env = dag.get_template_env()
        for tarea in dag.tasks:
            try:
                texto = env.from_string(tarea.bash_command).render(
                    data_interval_start=inicio, data_interval_end=fin)
            except Exception as exc:  # noqa: BLE001 - se reporta cualquier error
                errores.append(f"template de {tarea.task_id}: {exc}")
                continue
            if "{{" in texto:
                errores.append(f"template de {tarea.task_id} sin resolver")
        ingesta = env.from_string(dag.get_task("ingerir_bronze").bash_command)
        dbt = env.from_string(dag.get_task("dbt_run").bash_command)
        if '--inicio "2026-11-09T21:00:00+00:00"' not in ingesta.render(
                data_interval_start=inicio, data_interval_end=fin):
            errores.append("la ingesta no recibe el intervalo de la corrida")
        if '"desde": "2026-11-09"' not in dbt.render(
                data_interval_start=inicio, data_interval_end=fin):
            errores.append("dbt no recibe el rango de dias de la corrida")

    if errores:
        print("CI ROJO:", *errores, sep="\n  - ")
        sys.exit(1)
    print(f"OK: {len(bag.dags)} DAG(s) importan; pipeline_grupo03 bien armado "
          "y sus templates renderizan.")


if __name__ == "__main__":
    main()
