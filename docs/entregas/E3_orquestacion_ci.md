# Entrega 3 — Orquestación con Airflow e integración continua

**Grupo 03 · Ingeniería de Datos · LCD · UCA Rosario · 2026**

Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés

Repositorio: https://github.com/choco721/INGDD-UCA2026

DAG: `dags/pipeline_grupo03.py` · CI: `.github/workflows/ci.yml`

---

## Qué hace

El DAG `pipeline_grupo03` encadena todo el pipeline y corre solo todos los
días a las 18:00 (hora de Argentina):

```
preparar_entorno → ingerir_bronze → dbt_run → dbt_test → publicar_gold
```

Cada cambio que se sube al repositorio dispara un CI en GitHub Actions que
verifica el DAG, el SQL y los tests de dbt, y que bloquea el merge si algo
falla.

## 1. Cómo levantar Airflow y dónde ver el DAG

El pipeline corre en el **servidor de la cátedra**: Airflow, MinIO, PostgreSQL
y Metabase ya están levantados ahí. El servidor le monta a Airflow la carpeta
`dags/` del grupo en `/opt/airflow/dags/grupo03`.

**Desplegar una versión del repo en el servidor** (desde una celda de
Jupyter, con el clon del repo en `~/work/INGDD-UCA2026`):

```python
!python /home/jovyan/work/INGDD-UCA2026/herramientas/desplegar.py
```

El script copia al servidor solo los archivos que cambiaron. Antes de pisar
nada guarda un respaldo, nunca toca `.env` ni la base, y al final corre un
chequeo: limpia los archivos de respaldo de Jupyter, revisa permisos, compila
el DAG y parsea dbt. Si el chequeo termina en `LISTO`, Airflow toma la versión
nueva en menos de un minuto.

**Dónde ver el DAG:** en la interfaz de Airflow del servidor, buscando
`pipeline_grupo03` (etiquetas `grupo03`, `vatsim`, `proyecto-integrador`). La
vista **Grid** muestra el historial de corridas tarea por tarea; la vista
**Graph**, el grafo de dependencias.

> El DAG usa rutas del servidor (`/opt/airflow/dags/grupo03`), así que no
> corre tal cual en el stack local del repo. Las piezas sí corren por separado
> en local: la ingesta (E1) y dbt (E2).

## 2. Qué hace cada tarea y por qué están separadas

| Tarea | Qué hace | Si falla, se sabe que… |
|---|---|---|
| `preparar_entorno` | Crea o reutiliza un entorno virtual con las dependencias del grupo (la imagen de Airflow del servidor no trae dbt ni deltalake). | …faltan dependencias. No se tocó ningún dato. |
| `ingerir_bronze` | Trae la foto de VATSIM y la agrega a bronze si es nueva. Recibe el intervalo de la corrida. | …el problema es la API o MinIO. |
| `dbt_run` | Construye silver y gold. Recibe el rango de días de la corrida. | …el problema es SQL o los datos de bronze. |
| `dbt_test` | Corre los 33 tests de calidad sobre lo construido. | …los datos están mal: no se publican. |
| `publicar_gold` | Copia las seis tablas gold a PostgreSQL, para Metabase. | …el problema es la conexión al warehouse. |

**Por qué esta granularidad.** Cada tarea toca un sistema distinto y falla por
motivos distintos. Separadas, el grafo dice dónde está el problema sin abrir
un log, y un reintento repite solo lo que falló: si cae la publicación, no se
vuelve a llamar a VATSIM ni a reconstruir dbt.

`dbt_run` y `dbt_test` están separadas a propósito, aunque `dbt build` hace
las dos cosas juntas: así un dato que no pasa los tests corta el pipeline
**antes** de publicarse. Si `dbt_test` falla, `publicar_gold` no corre y el
tablero sigue mostrando el último dato bueno, en lugar de uno malo.

**Nada pesado en el nivel superior del archivo.** El DAG solo arma strings con
comandos: no consulta la base, ni la API, ni lee archivos al importarse.

**Schedule y start_date.** Una corrida diaria a las 18:00. La hora fija
importa: el tráfico de VATSIM cambia mucho según la hora del día, y observar
siempre a la misma hora hace comparables los días entre sí. El `start_date`
es el 22/10/2026, el día de la primera ingesta real. `catchup=False` evita
que, al reactivar el DAG, Airflow dispare de golpe una corrida por cada día
que estuvo pausado: si hace falta reprocesar el pasado, se hace a propósito
con un backfill.

## 3. Reintentos e idempotencia

`default_args`: **2 reintentos, separados 1 minuto.**

- *Qué falla esperamos que resuelvan:* que VATSIM, MinIO o PostgreSQL no
  respondan durante unos minutos. Dos reintentos espaciados cubren eso.
- *Por qué no más:* si después de tres intentos sigue fallando, no es un corte
  pasajero. Diez reintentos solo esconderían el problema durante media hora.
- *Por qué no cero:* la API externa se cae de verdad, y una corrida perdida por
  un corte de un minuto es un día menos en la serie.
- *Lo que el reintento no puede arreglar:* credenciales mal puestas o una API
  que cambió de formato. La ingesta los detecta como falla permanente (E1) y
  falla rápido con un mensaje claro.

**Reintentar es seguro porque cada tarea es idempotente.** La ingesta no
escribe una foto que ya está; `dbt_run` reconstruye siempre lo mismo a partir
de lo mismo; `publicar_gold` reemplaza las tablas enteras. Si una tarea falla
a mitad de camino y se reintenta, el resultado es el mismo que si hubiera
salido bien la primera vez. Sin idempotencia, los reintentos serían
peligrosos: cada uno duplicaría datos.

## 4. Backfill

### Qué significa reprocesar el pasado con esta fuente

VATSIM no guarda histórico: no se le puede pedir "la foto del 9 de
noviembre". Pero **bronze sí guarda todo lo que ingerimos**. Entonces el
backfill reprocesa el pasado **desde bronze**:

- `ingerir_bronze` recibe el intervalo de la corrida (`data_interval_start` y
  `data_interval_end`, del contexto de Airflow). Si el intervalo es pasado, la
  foto que puede dar VATSIM no le pertenece: no ingiere nada, lo dice en el log
  y termina en verde. Si bronze ya tiene datos posteriores al intervalo, ni
  siquiera llama a la API. La decisión sale de comparar el intervalo con los
  datos, **nunca con el reloj**.
- `dbt_run` recibe los días del intervalo (`--vars '{"desde": ..., "hasta": ...}'`)
  y reconstruye **solo esos días** de `fct_trafico_diario`: borra las filas de
  esas fechas y las vuelve a calcular desde bronze. Reprocesar un día dos veces
  da el mismo resultado.
- Las tablas de "estado actual" (posiciones, rutas, aeronaves de la última
  foto) se reconstruyen enteras en cada corrida: no tienen intervalo.

El rango incluye el día en que empieza el intervalo y el día en que termina.
Reprocesar también el día anterior incorpora fotos tomadas después de su
corrida (por ejemplo, una ingesta manual a la noche). Como es idempotente, el
solapamiento no duplica nada.

### Cómo dispararlo

**Opción A — interfaz de Airflow, un período elegido en el momento.**
1. En `pipeline_grupo03`, botón ▶ → **Trigger DAG w/ config**.
2. En **Logical date**, elegir una fecha pasada (por ejemplo, 2026-11-08 18:00).
3. **Trigger**.

**Opción B — reprocesar corridas que ya existen.** En la vista Grid, elegir la
corrida de un día pasado → clic en `ingerir_bronze` → **Clear**, con
**Downstream** tildado. Sirve, por ejemplo, para la semana del 6 al 10/11, en
que `dbt_run` falló cinco días seguidos.

**Opción C — línea de comandos**, si se tiene acceso al contenedor de Airflow:

```bash
airflow dags backfill pipeline_grupo03 -s 2026-11-06 -e 2026-11-09
```

### Dónde mirar que funcionó

1. **Log de `ingerir_bronze`:** tiene que decir que el intervalo es anterior al
   watermark y que no hay nada que ingerir. No escribe nada.
2. **Log de `dbt_run`:** el comando muestra los días recibidos, por ejemplo
   `--vars '{"desde": "2026-11-08", "hasta": "2026-11-09"}'`. Esos días son los
   del período elegido, no los de hoy.
3. **En PostgreSQL o Metabase:** `select * from grupo03.fct_trafico_diario
   order by fecha`. Las filas de esos días se recalcularon; las demás no
   cambiaron.

Probado con la muestra de CI: se modificaron en bronze los días 23 y 24/10, se
reprocesó solo el 23, y cambió solo el 23. Repetir el mismo reproceso dio el
mismo resultado.

### ¿Y si quisiéramos backfillear dos años?

Con el diseño actual serían unas 730 corridas en serie (`max_active_runs=1`).
La ingesta no llamaría a VATSIM en ninguna, porque el watermark ya es posterior
a todos esos días. Pero cada corrida reconstruye las tablas de estado actual y
paga el arranque de dbt: varias horas para un resultado que se puede obtener
de otra forma. Además, nuestro bronze empieza el 22/10/2026: antes de esa fecha
no hay nada que reprocesar.

Lo que haríamos: no usar Airflow para eso. Una sola ejecución de dbt sobre todo
el rango hace el mismo trabajo en una pasada:

```bash
dbt run --select +fct_trafico_diario+ --vars '{"desde": "2024-11-01", "hasta": "2026-11-17"}'
```

O `dbt run --full-refresh`, que reconstruye la serie entera desde bronze. El
backfill por corridas sirve para unos días puntuales; para períodos largos, una
pasada.

## 5. Integración continua

### Qué verifica

`.github/workflows/ci.yml` corre en cada push y en cada pull request. Son dos
trabajos en paralelo, de unos dos minutos:

| Trabajo | Qué verifica |
|---|---|
| **DAG importa y está bien armado** (`ci/test_dag.py`, con Airflow 2.10.5) | Que todos los DAGs importen sin errores. Que `pipeline_grupo03` tenga las cinco tareas encadenadas en orden y reintentos configurados. Que los templates de Jinja rendericen con un intervalo pasado y que la ingesta y dbt reciban ese intervalo: un error de template no aparece al importar, aparece recién cuando Airflow corre la tarea. |
| **SQL válido y tests de dbt en verde** | `dbt parse` (YAML, Jinja, referencias) y `dbt build` completo —8 modelos y 33 tests— contra una base DuckDB descartable y la muestra de bronze. Usa las mismas versiones de dbt y DuckDB que el servidor, leídas de `requirements_grupo03.txt`. |

No usa el lake ni credenciales: no hay ningún secreto en el workflow.

### Qué pasa si falla

El commit o el pull request queda marcado en rojo, con el detalle del
error en el log del trabajo. Y como `main` está protegida, **el pull request
no se puede mergear** hasta que el CI pase.

**Configuración de la protección** (una vez, en GitHub): *Settings → Branches
→ Add branch protection rule* para `main`, con *Require a pull request before
merging* y *Require status checks to pass before merging*, eligiendo los dos
trabajos del CI. Sin esto, el CI informa pero no bloquea.

### Demostración del CI en rojo

Dos ramas con un error realista cada una:

- **Romper una dependencia del DAG** (`dbt_test` deja de esperar a `dbt_run`):
  ```
  CI ROJO:
    - dbt_test no depende de dbt_run
  ```
- **Sacar la deduplicación de silver** (comentar `where rn = 1` en
  `stg_vatsim.sql`):
  ```
  9 of 41 FAIL 1 unique_stg_vatsim_snapshot_sk ....... [FAIL 1]
  Done. PASS=8 WARN=0 ERROR=1 SKIP=32
  ```

En las dos, el pull request a `main` queda bloqueado.

> [CAPTURA E3-1] Grid del DAG con el historial de corridas.
> [CAPTURA E3-2] Graph del DAG.
> [CAPTURA E3-3] Log de `dbt_run` de un backfill, con los días recibidos.
> [CAPTURA E3-4] El CI en verde en GitHub Actions.
> [CAPTURA E3-5] El pull request bloqueado por el CI en rojo.
