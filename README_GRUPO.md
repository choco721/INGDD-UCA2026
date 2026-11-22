# Pipeline de datos VATSIM — grupo03

**Ingeniería de Datos · LCD · UCA Rosario · 2026**

**Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés**

Pipeline que ingesta tráfico aéreo simulado desde la API de VATSIM, lo
transforma en capas con dbt y publica métricas en PostgreSQL para un tablero de
Metabase. Lo orquesta Airflow: corre solo, todos los días a las 18:00, en el
servidor de la cátedra. Cada cambio al repositorio pasa por un CI que lo
verifica antes de poder integrarse.

## Arquitectura

```
API VATSIM
    │  dags/scripts/ingesta.py        incremental, idempotente, recibe el intervalo
    ▼
BRONZE   Delta Lake en MinIO  s3://grupo03/bronze/vatsim
    │  dbt sobre DuckDB  (dags/dbt/)
    ▼
SILVER   stg_vatsim                  vista: tipado, nulos, deduplicado
    ▼
GOLD     fct_trafico_snapshot → fct_trafico_diario → fct_features_trafico_diario
         fct_rutas · fct_aeronaves · fct_posiciones
    │  dags/scripts/publicar_gold.py  una transacción, reemplazo total
    ▼
PostgreSQL warehouse.grupo03  ──►  Metabase

Orquestación: dags/pipeline_grupo03.py
  preparar_entorno → ingerir_bronze → dbt_run → dbt_test → publicar_gold
CI: .github/workflows/ci.yml  (DAG + dbt build contra una muestra)
```

## Documentación de cada entrega

| Entrega | Documento |
|---|---|
| E1 · Ingesta idempotente | [docs/entregas/E1_ingesta.md](docs/entregas/E1_ingesta.md) |
| E2 · dbt y calidad | [docs/entregas/E2_dbt.md](docs/entregas/E2_dbt.md) |
| E3 · Airflow y CI | [docs/entregas/E3_orquestacion_ci.md](docs/entregas/E3_orquestacion_ci.md) |
| E4 · Serving, features, governance | [docs/entregas/E4_serving_features_governance.md](docs/entregas/E4_serving_features_governance.md) |
| Governance del dataset | [docs/governance/governance_vatsim.md](docs/governance/governance_vatsim.md) |
| Tablero: preguntas y SQL | [docs/tablero/preguntas_tablero.md](docs/tablero/preguntas_tablero.md) |

## Estructura del repositorio

```
.github/workflows/ci.yml     CI: verifica DAG, SQL y tests en cada push
ci/test_dag.py               controles del DAG que corre el CI
dags/                        lo que el servidor le monta a Airflow
├── pipeline_grupo03.py      el DAG
├── entorno.py               lee la configuración del .env
├── .env.example             variables requeridas, sin valores
├── .airflowignore           lo que no es un DAG
├── preparar_entorno_grupo03.sh, requirements_grupo03.txt
│                            entorno virtual propio (la imagen del servidor no trae dbt)
├── scripts/                 ingesta.py, publicar_gold.py
└── dbt/                     proyecto dbt: models/, tests/, ci/ (muestras para el CI)
herramientas/
├── desplegar.py             copia una versión del repo al servidor, con respaldo
└── verificar_despliegue.py  chequeo previo a cada corrida
docs/                        entregas, governance, tablero
notebooks/                   prácticas de clase y bitácora
```

Las carpetas `dbt/`, `scripts/`, `docker/` de la raíz y `docker-compose.yml`
son el stack de ejemplo de la cátedra.

## Trabajar sobre el pipeline

El repositorio es la fuente de verdad. La carpeta del servidor no se edita a
mano: editar ahí desde Jupyter dejó archivos de respaldo que Airflow y dbt
leyeron como código, y rompió el pipeline dos veces.

1. Cambio en una rama → push → el CI tiene que quedar en verde.
2. Pull request a `main` (protegida: no se mergea con el CI en rojo).
3. En el servidor: `git pull` en `~/work/INGDD-UCA2026` y
   `python herramientas/desplegar.py`.

---

## Anexo — Preguntas de cierre de la actividad integradora (27/10)

**1. ¿Por qué Airflow no debería depender de la ejecución manual de un notebook?**

Porque un notebook requiere que una persona abra el entorno y ejecute celdas en
el orden correcto, lo que hace el proceso irreproducible y no automatizable.
Además, el estado de un notebook depende del orden real de ejecución y no del
orden visual de las celdas, así que el mismo archivo puede dar resultados
distintos. Airflow necesita artefactos ejecutables de forma no interactiva, con
código de salida verificable y logs que permitan diagnosticar fallas. Los
notebooks quedan como evidencia del análisis; la lógica operativa vive en
`scripts/` y en el proyecto dbt.

**2. ¿Qué responsabilidad tiene `scripts/ingesta.py` y qué responsabilidad tiene la DAG?**

`ingesta.py` contiene la lógica: leer la configuración, llamar a la API,
validar la respuesta, normalizar los registros, aplicar el criterio incremental
y escribir en bronze sin duplicar. Es autónomo y se puede probar solo.

La DAG no contiene lógica de negocio: coordina. Define qué se ejecuta, en qué
orden, con qué dependencias, cuántos reintentos y bajo qué condiciones. Si la
DAG desapareciera, los scripts seguirían funcionando desde la terminal; esa es
la prueba de que las responsabilidades están bien separadas.

**3. ¿Qué parte del procesamiento corresponde a dbt y cuál no?**

A dbt le corresponde la transformación dentro del warehouse: tipado, limpieza,
deduplicado, cálculo de métricas y validación de calidad, todo expresado en SQL
sobre datos que ya están disponibles.

No le corresponde la extracción desde la API ni la escritura inicial en el lake
(eso es ingesta, en Python), ni la publicación hacia PostgreSQL (eso es
serving). dbt transforma lo que ya está; no trae ni entrega datos.

**4. ¿Cómo garantiza el grupo que una segunda ejecución no duplica información?**

Con las dos barreras descritas en `docs/entregas/E1_ingesta.md`: el watermark sobre
`snapshot_ts` evita volver a traer una foto ya ingerida, y el anti-join por la
clave natural `(cid, snapshot_ts)` descarta lo que ya existe aunque el snapshot
llegue repetido. La verificación está documentada: dos corridas consecutivas, la
segunda devuelve 0 filas y código de salida 0.

A esto se suma el test de unicidad en dbt, que falla si por cualquier motivo
llegara a existir un duplicado en la capa staging.

**5. ¿Qué prueba de dbt resultó más importante para los datos del grupo y por qué?**

La unicidad de `(cid, snapshot_ts)`. Es la regla que un incidente de reingesta
violó durante el simulacro, y es la que sostiene toda la capa de marts: las
métricas se calculan con `count(distinct cid)` agrupando por snapshot, de modo
que un duplicado inflaría el conteo de pilotos conectados y distorsionaría la
variación porcentual sin que nada avisara.

Mención aparte para el test de altitud, que fue valioso por el motivo opuesto:
falló y al investigarlo resultó que la regla estaba mal planteada, no los datos.

**6. ¿Qué información aporta el log de una tarea de Airflow cuando el pipeline falla?**

El comando exacto que se ejecutó, la salida estándar y de error del proceso, el
código de salida y el traceback completo cuando se trata de Python. Eso permite
distinguir en qué capa está el problema: configuración, dependencias, acceso a
datos o lógica.

En este proyecto fue decisivo. El log nos llevó, en sucesivas iteraciones, desde
un error de permisos del archivo `.env` hasta el `ModuleNotFoundError: No module
named 'deltalake'`, que identificó la causa real como una dependencia ausente en
la imagen del contenedor y no un problema del código del grupo. Sin el log
habríamos estado modificando el pipeline a ciegas.
