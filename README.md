# Pipeline de datos VATSIM — grupo03

**Ingeniería de Datos · LCD · UCA Rosario · 2026**

Pipeline reproducible que ingesta tráfico aéreo simulado desde la API de VATSIM,
lo transforma en capas con dbt y publica métricas de negocio en PostgreSQL para
consumo desde Metabase. Todo el proceso se orquesta con Airflow, corre a diario
de forma automática y no depende de la ejecución manual de ningún notebook.

## 1. Fuente de datos

**API**: `https://data.vatsim.net/v3/vatsim-data.json` — red de tráfico aéreo
simulado. Pública, sin API key y sin cuota de uso.

Particularidad de la fuente: VATSIM **no ofrece histórico ni filtro por fecha**.
Cada llamada devuelve una foto del momento (se actualiza cada ~15 segundos) con
todos los pilotos conectados. El histórico se construye acumulando snapshots en
la capa bronze: una corrida equivale a una foto, identificada por `snapshot_ts`
(el campo `general.update_timestamp` de la respuesta).

## 2. Arquitectura

```
API VATSIM
    |  scripts/ingesta.py  (incremental + idempotente)
    v
bronze: tabla Delta en MinIO  s3://grupo03/bronze/vatsim
    |  dbt (DuckDB)
    v
staging: brz_vatsim -> stg_vatsim     (vistas: tipado, limpieza, deduplicado)
    |
    v
marts: fct_trafico_snapshot   fct_rutas   fct_aeronaves   fct_posiciones
    |  scripts/publicar_gold.py
    v
PostgreSQL: esquema warehouse.grupo03  -> Metabase
```

## 3. Estructura del repositorio

```
grupo03/
├── .env.example           variables requeridas, sin valores
├── entorno.py             resolución de configuración y credenciales
├── README_GRUPO.md
├── notebooks/             análisis exploratorio y evidencia de la entrega
├── scripts/ -> dags/scripts/    (enlace)
├── dbt/     -> dags/dbt/        (enlace)
└── dags/                  única raíz real; es lo que monta Airflow
    ├── .env               credenciales (no se versiona)
    ├── .airflowignore     evita que Airflow parsee módulos que no son DAGs
    ├── entorno.py
    ├── pipeline_grupo03.py
    ├── preparar_entorno_grupo03.sh
    ├── requirements_grupo03.txt
    ├── .runtime_grupo03/  entorno virtual del grupo (no se versiona)
    ├── scripts/
    │   ├── __init__.py
    │   ├── ingesta.py
    │   └── publicar_gold.py
    └── dbt/
        ├── dbt_project.yml
        ├── profiles.yml
        ├── models/
        │   ├── schema.yml
        │   ├── staging/   brz_vatsim.sql, stg_vatsim.sql
        │   └── marts/     fct_trafico_snapshot.sql, fct_rutas.sql,
        │                  fct_aeronaves.sql, fct_posiciones.sql
        └── tests/         assert_latitud_valida.sql
                           assert_longitud_valida.sql
                           assert_altitud_valida.sql
```

**Por qué `dbt/` y `scripts/` son enlaces**: el servidor monta únicamente la
carpeta `dags/` del grupo dentro del contenedor de Airflow
(`/opt/airflow/dags/grupo03`). Para que exista una sola copia de cada cosa, los
archivos reales viven ahí y la raíz expone enlaces, de modo que el trabajo desde
Jupyter y la ejecución desde Airflow usan exactamente los mismos archivos.

## 4. Variables de entorno requeridas

Se cargan desde `.env` (ver `.env.example`) o del entorno del servidor. Ninguna
credencial está escrita en el código.

| Variable | Para qué |
|---|---|
| `MINIO_BUCKET` | bucket del grupo en el lake |
| `AWS_ACCESS_KEY_ID` | credencial de MinIO |
| `AWS_SECRET_ACCESS_KEY` | credencial de MinIO |
| `AWS_ENDPOINT_URL` | endpoint de MinIO |
| `PGHOST`, `PGPORT` | servidor PostgreSQL de serving |
| `PGDATABASE`, `PGUSER`, `PGPASSWORD` | base y credenciales de serving |

## 5. Reproducir el pipeline desde cero

```bash
cd /home/jovyan/work

# 1. Ingesta incremental hacia bronze
python -m scripts.ingesta

# 2. Transformación y validación
cd dbt
dbt debug  --profiles-dir .
dbt build  --profiles-dir .
cd ..

# 3. Publicación de la capa gold en PostgreSQL
python -m scripts.publicar_gold
```

O, en un solo paso, disparando el DAG `pipeline_grupo03` desde Airflow.

## 6. Estrategia incremental e idempotencia

La ingesta aplica **dos barreras independientes**:

1. **Watermark sobre `snapshot_ts`**: antes de llamar a la API se lee el máximo
   `snapshot_ts` ya presente en bronze. Si el snapshot que devuelve VATSIM no es
   más nuevo, la función devuelve vacío y no se escribe nada.
2. **Anti-join por clave natural**: aunque un snapshot repetido llegara a la
   función de escritura, se descartan las filas cuya combinación
   `(cid, snapshot_ts)` ya existe en bronze.

**Clave natural**: `cid + snapshot_ts` — un piloto (identificado por su `cid`,
único en VATSIM) aparece una sola vez por foto.

**Trazabilidad**: cada fila escrita lleva `_ingested_at` y `_run_id`, lo que
permite reconstruir qué corrida produjo cada registro (bronze es append-only).

Evidencia de la verificación:

```
PRUEBA 1 — ingesta
Filas extraidas: 1623 (snapshot 2026-10-23T20:49:09.0245399Z)
Ingeridas 1623 filas
exit code: 0

PRUEBA 2 — ingesta de nuevo
Sin snapshot nuevo (watermark=2026-10-23T20:49:09.0245399Z).
Filas ingeridas: 0
exit code: 0
```

## 7. Modelos dbt

| Modelo | Capa | Materialización | Grano | Qué hace |
|---|---|---|---|---|
| `brz_vatsim` | staging | vista | fila cruda | Lee la tabla Delta del lake a través de la source `lake.vatsim` con `delta_scan`. No transforma. |
| `stg_vatsim` | staging | vista | `cid` × `snapshot_ts` | Tipa las columnas (en bronze todo llega como texto), normaliza a NULL los campos de plan de vuelo vacíos, descarta filas sin clave y deduplica. Expone `snapshot_sk` como clave sustituta. |
| `fct_trafico_snapshot` | marts | tabla | `snapshot_ts` | Métricas por foto: pilotos conectados, pilotos sin plan de vuelo, aeropuertos de origen y destino distintos, y variación porcentual contra el snapshot anterior. |
| `fct_rutas` | marts | tabla | `snapshot_ts` × origen × destino | Vuelos por ruta. Responde qué rutas y qué aeropuertos concentran el tráfico. |
| `fct_aeronaves` | marts | tabla | `snapshot_ts` × aeronave | Composición de la flota por tipo de avión, con altitud y velocidad medias. |
| `fct_posiciones` | marts | tabla | `cid` del último snapshot | Posición geográfica de cada piloto conectado, con una fase de vuelo derivada de la altitud. Alimenta el mapa del tablero. |

**staging vs marts**: staging hace limpieza y tipado sin lógica de negocio, 1:1
con la fuente, y se materializa como vista porque se recalcula barato y no
necesita persistir. Marts contiene los modelos orientados al consumo y se
materializan como tabla, de modo que Metabase los lea rápido y sin depender del
acceso al lake.

**Por qué `fct_posiciones` se limita al último snapshot**: es una foto del
estado actual, no una serie. Acotarla mantiene la tabla en el orden de 1.600
filas en lugar de crecer con cada corrida, que es lo que un mapa necesita.

## 8. Tests de calidad

Las dos reglas obligatorias salieron de un simulacro de incidente: se reprodujo
una reingesta duplicada sobre una copia del lake y se auditó con time travel de
Delta.

| Test | Sobre | Regla |
|---|---|---|
| `unique` + `not_null` en `snapshot_sk` | stg_vatsim | Unicidad de la clave natural `(cid, snapshot_ts)` |
| `not_null` en `cid` y `snapshot_ts` | stg_vatsim | Sin la clave, el registro no es identificable |
| `assert_latitud_valida` | stg_vatsim | Latitud dentro de [-90, 90] |
| `assert_longitud_valida` | stg_vatsim | Longitud dentro de [-180, 180] |
| `assert_altitud_valida` | stg_vatsim | Altitud no menor a -1500 ft |
| `unique` + `not_null` en `snapshot_ts` | fct_trafico_snapshot | El grano del hecho es un snapshot |
| `not_null` en `pilotos_conectados` | fct_trafico_snapshot | La métrica no puede faltar |
| `not_null` en origen, destino y vuelos | fct_rutas | Una ruta sin extremos no es una ruta |
| `not_null` en aeronave y vuelos | fct_aeronaves | El grano exige el tipo de avión |
| `unique` + `not_null` en `cid` | fct_posiciones | Un piloto, una posición por snapshot |
| `not_null` en `latitude` y `longitude` | fct_posiciones | Sin coordenadas el punto no se puede mapear |
| `accepted_values` en `fase_vuelo` | fct_posiciones | La derivación solo puede producir cuatro valores |

Sobre el umbral de altitud: la regla candidata original era "altitud no
negativa", pero el test detectó 10 violaciones. Al investigarlas resultaron ser
valores de -2 a -7 ft, correspondientes a aviones en tierra (el altímetro
reporta altitud MSL según la presión QNH, y hay aeropuertos bajo el nivel del
mar). La regla estaba mal planteada, así que se ajustó a un umbral físico de
-1500 ft en lugar de silenciar el test.

Estado actual: **29 nodos, PASS=29, ERROR=0** (4 tablas, 2 vistas, 23 tests).

## 9. Orquestación

DAG `pipeline_grupo03`, cinco tareas encadenadas con `BashOperator`:

```
preparar_entorno -> ingerir_bronze -> dbt_run -> dbt_test -> publicar_gold
```

Decisiones:

- **`schedule="0 18 * * *"`**: una corrida diaria a las 18:00 de Córdoba. La
  hora fija importa: el tráfico de VATSIM varía fuertemente según la hora del
  día, así que observar siempre a la misma hora hace comparables los snapshots
  entre jornadas. Una frecuencia mayor acumularía la serie más rápido, pero
  mezclaría franjas horarias y volvería engañosa cualquier comparación diaria.
- **`catchup=False`**: no tiene sentido recuperar corridas pasadas, porque la
  API solo devuelve el presente. Un backfill traería la foto de hoy etiquetada
  como si fuera de ayer.
- **`max_active_runs=1`**: DuckDB no admite escrituras concurrentes sobre
  `warehouse.duckdb`; dos corridas superpuestas se bloquearían entre sí.
- **`dbt run` y `dbt test` en tareas separadas** (en vez de un solo `dbt build`):
  así el grafo distingue si falló la construcción o la validación, y la
  publicación no se ejecuta si un test está en rojo.
- **`retries: 2` con un minuto de espera**: la API de VATSIM puede fallar de
  forma transitoria; un reintento resuelve la mayoría de esos casos.
- Cada tarea carga el `.env` y expone la raíz del grupo en `PYTHONPATH`, de modo
  que los scripts encuentren `entorno.py` sin rutas escritas en el código.

### Primera tarea: `preparar_entorno`

La imagen de Airflow del servidor no incluye `dbt-core`, `dbt-duckdb`, `duckdb`
ni `deltalake`. Se diagnosticó con un DAG que inspeccionaba el contenedor y se
reportó al docente, que autorizó al grupo a resolverlo por su cuenta sin
reconstruir la imagen compartida.

La solución es un entorno virtual propio dentro de la carpeta montada
(`dags/.runtime_grupo03/venv`), que `preparar_entorno_grupo03.sh` crea en la
primera corrida y reutiliza en las siguientes. El script usa `flock` para evitar
que dos corridas lo construyan a la vez, y guarda un archivo marcador con el
hash del `requirements` y la versión de Python: si ninguno de los dos cambió,
salta la instalación. Las tareas siguientes anteponen ese `venv` al `PATH`.

No es lo que uno haría en producción —ahí la respuesta correcta es que las
dependencias estén declaradas en la imagen—, pero resuelve el bloqueo sin tocar
un recurso compartido por todos los grupos.

## 10. Consumo

`scripts/publicar_gold.py` publica las cuatro tablas de marts en el esquema
`grupo03` de la base `warehouse` en PostgreSQL. La estrategia es de reemplazo
total en cada corrida: la capa gold se reconstruye entera, de modo que el
tablero siempre refleja el último estado del pipeline. Antes de escribir, el
script verifica que la conexión apunte a la base y al usuario esperados, y
después compara la cantidad de filas leídas contra las publicadas.

Sobre esas tablas se construyó el tablero **VATSIM · Tráfico aéreo en tiempo
real** en Metabase (colección *Grupo03 · VATSIM*):

| Panel | Fuente | Qué muestra |
|---|---|---|
| Posición actual de pilotos conectados | `fct_posiciones` | Mapa de pines con los ~1.600 pilotos del último snapshot |
| Top 10 rutas | `fct_rutas` | Rutas con más tráfico en la última foto |
| Mix de flota por tipo de aeronave | `fct_aeronaves` | Participación de cada tipo, con la cola larga agrupada |
| Distribución por fase de vuelo | `fct_posiciones` | En tierra / ascenso o descenso / crucero |
| Evolución del tráfico | `fct_trafico_snapshot` | Serie de pilotos conectados por snapshot |
| Datos publicados | las cuatro | Conteo de filas por tabla, como control de publicación |

## 11. Hallazgo de calidad de datos

El panel de rutas dejó a la vista una anomalía en la fuente. En el snapshot del
3 de noviembre, la ruta `GVAC -> DNMM` (Sal, Cabo Verde → Lagos, Nigeria)
aparece con 25 vuelos, muy por encima del resto, y `GVSV -> DNMM` con 10.

Al cruzarlo con `fct_posiciones`, varios de esos pilotos están exactamente en
`16.74, -22.95` —las coordenadas del aeropuerto de Sal—, con `groundspeed = 0` y
unos 190 ft de altitud. Es decir: dos docenas de aeronaves inmóviles en el mismo
punto declarando el mismo destino.

Las explicaciones posibles son un evento organizado de la red, pilotos
conectados y ausentes, o un punto de aparición por defecto de algún cliente. No
se determinó cuál. **No se filtró el dato**: descartar registros sin entender la
causa introduciría un sesgo peor que el que corrige.

Queda documentado como regla candidata para un contrato de datos: marcar como
sospechoso todo plan de vuelo que mantenga `groundspeed = 0` a lo largo de
varios snapshots consecutivos. Implementarla requiere una serie más larga que la
disponible al momento de la entrega.

## 12. Preguntas de cierre

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

Con las dos barreras descritas en la sección 6: el watermark sobre
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
