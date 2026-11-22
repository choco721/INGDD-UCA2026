# Entrega 1 — Ingesta idempotente hacia la capa bronze

**Grupo 03 · Ingeniería de Datos · LCD · UCA Rosario · 2026**

Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés

Repositorio: https://github.com/choco721/INGDD-UCA2026

Código: `dags/scripts/ingesta.py`

---

## Qué hace

Trae de la API de VATSIM (`https://data.vatsim.net/v3/vatsim-data.json`) la
lista de pilotos conectados a la red de tráfico aéreo simulado, y la agrega a
una tabla Delta en MinIO: `s3://grupo03/bronze/vatsim`.

VATSIM no tiene histórico ni filtro por fecha. Cada llamada devuelve **una
foto del momento** (se renueva cada ~15 segundos) con todos los pilotos
conectados. El histórico se construye acumulando fotos: una corrida, una foto.

Bronze guarda la foto **tal cual vino**: todas las columnas como texto, sin
limpiar, más dos de trazabilidad (`_ingested_at` y `_run_id`). Limpiar es
trabajo de silver; si una regla de limpieza resulta equivocada, el dato
original sigue en bronze para reprocesar.

## 1. Cómo correrlo, desde cero

La ingesta lee las credenciales de variables de entorno: nunca están en el
código. `dags/entorno.py` las toma de un archivo `.env` al lado suyo.

```bash
git clone https://github.com/choco721/INGDD-UCA2026.git
cd INGDD-UCA2026/dags

cp .env.example .env        # completar con las credenciales (ver abajo)

python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements_grupo03.txt

python -m scripts.ingesta   # primera corrida: trae la foto actual
python -m scripts.ingesta   # segunda corrida: no agrega nada
```

**Qué poner en `.env`:**

- *En el servidor de la cátedra:* las credenciales del grupo
  (`MINIO_BUCKET=grupo03` y el resto, provistas por la cátedra).
- *En una máquina propia:* levantar el stack local del repo
  (`docker compose up -d minio minio-setup`) y usar
  `MINIO_BUCKET=lake`, `AWS_ENDPOINT_URL=http://localhost:9000`,
  `AWS_ACCESS_KEY_ID=minioadmin`, `AWS_SECRET_ACCESS_KEY=minioadmin`.

Si falta alguna variable, el script corta antes de empezar y dice cuál.

## 2. El watermark: qué campo y por qué

**Campo:** `snapshot_ts`, el `general.update_timestamp` de la respuesta: el
momento en que VATSIM generó la foto.

**Por qué ese:** es la única marca de tiempo que identifica a la foto
completa, y la pone el origen, no nosotros. Si usáramos la hora de la
corrida, dos corridas seguidas tendrían horas distintas aunque trajeran la
misma foto.

**De dónde sale:** del máximo `snapshot_ts` que ya está en bronze. No hay un
archivo aparte con "la última fecha": el watermark se deriva de los datos, así
que no puede desincronizarse de ellos. En la primera corrida la tabla no
existe, el watermark es nulo y se escribe la foto completa.

**La limitación, y qué decidimos.** La consigna pide que la extracción le
pida al origen solo lo nuevo, pasando el watermark en la consulta. VATSIM no
lo permite: no acepta parámetros y no guarda pasado. Lo pensamos así: como la
fuente solo publica el presente, **cada llamada ya trae únicamente lo nuevo**:
no existe un histórico que estemos re-descargando. Lo que el watermark evita
es escribir dos veces la misma foto cuando dos corridas caen dentro de los
mismos 15 segundos, o cuando VATSIM tarda en renovarla. Antes de escribir, se
compara la foto recibida con el watermark; si no es más nueva, la corrida
termina sin tocar bronze.

## 3. Idempotencia

Correr la ingesta dos veces seguidas no duplica nada, y no porque se
deduplique al final: **la segunda corrida no escribe**, porque la foto que
recibe no es más nueva que el watermark.

Hay una segunda barrera, de seguridad: si dos corridas leyeran el watermark
al mismo tiempo (una de Airflow y otra manual desde Jupyter) y las dos
intentaran escribir la misma foto, antes de escribir se descartan las claves
`(cid, snapshot_ts)` que ya están en bronze para esa foto. Mira solo la foto
que se está por escribir, no toda la tabla.

**Clave natural:** `cid + snapshot_ts`. Un piloto (identificado por su `cid`,
único en VATSIM) aparece una sola vez por foto.

La escritura es `append`, nunca `overwrite`: reejecutar no puede borrar lo que
ya había.

## 4. Manejo de errores

El script distingue dos tipos de falla, porque se resuelven distinto:

| Tipo | Casos | Qué hace |
|---|---|---|
| **Transitoria** — puede resolverse sola | Timeout (30 s), sin conexión, HTTP 429 (demasiados pedidos), HTTP 500/502/503/504 | Reintenta hasta 3 veces en total, esperando 5 y 15 segundos. Si sigue fallando, termina con código de salida **1**. |
| **Permanente** — reintentar no sirve | Cualquier otro código HTTP (ej. 404), respuesta que no es JSON (ej. una página de mantenimiento), JSON sin `update_timestamp` o sin `pilots`, marca de tiempo ilegible, foto sin ningún piloto, pilotos sin `cid`/`latitude`/`longitude` | No reintenta. Termina enseguida con código de salida **2** y un mensaje que dice qué estaba mal. |

**Se verifica el estado antes de parsear.** Primero se mira el código HTTP;
solo un 200 se intenta leer como JSON. Un error del origen nunca se escribe
como si fueran datos.

**Una foto vacía no se escribe.** Una respuesta sin pilotos casi seguro es
una falla de la fuente, no una red sin tráfico (VATSIM nunca baja de cientos
de pilotos). Escribirla generaría un hueco falso en la serie.

**Dos niveles de reintento.** Los reintentos del script cubren cortes de
segundos. Cuando la ingesta corre en Airflow, la tarea además tiene 2
reintentos espaciados un minuto, que cubren cortes de minutos. Como la
ingesta es idempotente, reintentar nunca duplica.

**Si VATSIM cambia el formato.** Si desaparece un campo que necesitamos, la
validación lo detecta y la corrida termina como falla permanente, diciendo qué
falta. Si VATSIM *agrega* un campo, bronze lo incorpora como columna nueva
(`schema_mode="merge"`) en lugar de fallar: bronze guarda lo que vino, y decidir
si el campo importa es trabajo de silver.

**Cuando falla, no escribe nada parcial.** La foto se valida completa antes
de escribir, y la escritura en Delta es atómica: o entra la foto entera, o no
entra nada.

## 5. Evidencia

Las dos corridas consecutivas, con la ingesta real contra VATSIM:

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

> [CAPTURA E1-1] Las dos corridas seguidas con la versión actual del script.
> [CAPTURA E1-2] La tabla en MinIO: `grupo03/bronze/vatsim/` con `_delta_log/`.

Los casos de error de la tabla de la sección 4 se probaron uno por uno contra
una API simulada: el 503 reintenta 3 veces y termina con falla transitoria;
el 404 y la respuesta HTML terminan al primer intento como falla permanente;
la foto sin pilotos no se escribe.
