# Entrega 2 — Transformación con dbt y calidad de datos

**Grupo 03 · Ingeniería de Datos · LCD · UCA Rosario · 2026**

Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés

Repositorio: https://github.com/choco721/INGDD-UCA2026

Proyecto dbt: `dags/dbt/` (está dentro de `dags/` porque el servidor de la
cátedra le monta a Airflow únicamente esa carpeta).

---

## Qué hace

Transforma la tabla Delta de bronze —la que llena la ingesta de E1— en dos
capas, con dbt sobre DuckDB:

```
lake.vatsim (Delta en MinIO)
   └─ brz_vatsim            vista   bronze expuesto a dbt, sin transformar
       └─ stg_vatsim        vista   SILVER: un piloto en una foto, limpio y tipado
           ├─ fct_trafico_snapshot   tabla   ¿cuánta gente vuela en cada foto?
           │   └─ fct_trafico_diario incremental   ¿cuántos pilotos vuelan por día?
           │       └─ fct_features_trafico_diario  tabla   features (E4)
           ├─ fct_rutas              tabla   ¿qué rutas se vuelan más?
           ├─ fct_aeronaves          tabla   ¿qué aviones se vuelan más?
           └─ fct_posiciones         tabla   ¿dónde vuela la gente ahora?
```

**Silver lee de bronze real.** `brz_vatsim` lee la source `lake.vatsim`, que
es `delta_scan('s3://grupo03/bronze/vatsim')`. No hay seeds ni CSV en el
camino de producción. La única excepción es el CI (E3): GitHub no tiene acceso
a nuestro lake, así que ahí `brz_vatsim` lee una muestra de 71 filas con la
misma forma que bronze (`dags/dbt/ci/`). Esa rama del código solo se activa
pasando la variable `bronze_csv`; en el servidor nunca se pasa.

**Un solo modelo silver, y por qué alcanza.** Ingerimos una sola entidad: el
piloto conectado en una foto. La API también publica controladores, ATIS y
servidores, pero decidimos no ingerirlos porque ninguna pregunta del proyecto
los necesita. Separar `stg_vatsim` en dos modelos (por ejemplo, piloto y plan
de vuelo) daría dos tablas con el mismo grano unidas uno a uno: más modelos,
ninguna limpieza más.

## 1. Cómo correr el build, desde cero

```bash
git clone https://github.com/choco721/INGDD-UCA2026.git
cd INGDD-UCA2026/dags
cp .env.example .env               # credenciales del lake (ver E1)
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements_grupo03.txt
set -a; . ./.env; set +a           # exporta las variables al shell

cd dbt
export DBT_DUCKDB_PATH=$PWD/warehouse.duckdb
export DBT_S3_ENDPOINT=${AWS_ENDPOINT_URL#http://}
export DBT_S3_USE_SSL=false
dbt build --profiles-dir .         # modelos + tests, en orden
dbt docs generate --profiles-dir . --static   # linaje: target/static_index.html
```

Sin acceso al lake, el mismo build corre contra la muestra:

```bash
dbt build --profiles-dir . --target ci --vars '{bronze_csv: ci/vatsim_bronze_muestra.csv}'
```

## 2. Qué limpia silver

Cada decisión sale de algo que encontramos mirando los datos, no de un
manual.

| Problema encontrado en bronze | Qué decidimos | Por qué |
|---|---|---|
| **Todo llega como texto.** La ingesta guarda las columnas como string para no perder nada del original. | Se tipa cada campo: `cid` entero, coordenadas `double`, altitud/velocidad/rumbo enteros, `snapshot_ts` timestamp. | Sin tipos, "10000" < "9000" y no se puede promediar ni filtrar por rangos. |
| **El plan de vuelo vacío viene como `''`, no como nulo.** Un piloto sin plan de vuelo tiene `flight_plan_departure = ''`. | `nullif(..., '')` en origen, destino y aeronave. | Con `''`, `count(distinct origen)` cuenta "vacío" como si fuera un aeropuerto, y `origen is null` no detecta a los pilotos sin plan. |
| **Duplicados por reingesta.** En el simulacro de incidente se reingirió una foto sobre una copia del lake y quedaron filas repetidas. | Una sola fila por `(cid, snapshot_ts)`, con `row_number()`. Se expone `snapshot_sk` como clave y se testea `unique`. | La ingesta ya evita duplicar, pero silver no debe depender de que la capa anterior sea perfecta. Un duplicado inflaría el conteo de pilotos de todo gold. |
| **Filas sin piloto o sin foto.** | Se descartan si `cid` o `snapshot_ts` son nulos. | Sin clave, la fila no se puede identificar ni deduplicar. |
| **Altitudes negativas.** Aparecieron 10 filas entre -2 y -7 ft. | **No se limpian: son correctas.** Son aviones en tierra: la altitud es sobre el nivel del mar según la presión local, y hay aeropuertos bajo el nivel del mar. Se corrigió la regla del test (ver sección 4). | Limpiar un dato correcto porque contradice una regla mal pensada es peor que no limpiar. |
| **Nombres reales de personas.** El campo `name` muchas veces trae el nombre y apellido del piloto. | Queda en silver como `piloto`, de uso interno, y **no pasa a gold**: ninguna tabla publicada en PostgreSQL lo tiene. | Es un dato personal y ninguna pregunta del tablero lo necesita (ver governance, E4). |

## 3. Qué pregunta responde gold

La gold principal es `fct_trafico_diario`:

> **¿Cuántos pilotos vuelan por día en la red VATSIM, y cómo evoluciona eso?**

Una fila por día. Silver tiene unas 1.600 filas por foto; gold tiene una por
día. El resto de los modelos gold responden una pregunta cada uno:

| Modelo | Pregunta | Grano |
|---|---|---|
| `fct_trafico_snapshot` | ¿Cuánta gente vuela en cada foto y cuánto cambió contra la anterior? | una fila por foto |
| `fct_rutas` | ¿Cuáles son las rutas más voladas? | foto × ruta |
| `fct_aeronaves` | ¿Qué aviones se vuelan más? | foto × tipo de avión |
| `fct_posiciones` | ¿Dónde está volando la gente ahora mismo? | vuelo de la última foto |

**Vistas y tablas.** Bronze y silver son vistas: se recalculan barato y no
duplican datos. Gold son tablas: el tablero las lee seguido y tienen que
responder rápido, sin ir al lake en cada consulta. `fct_trafico_diario` es
incremental: en cada corrida reconstruye solo los días que le corresponden, lo
que habilita el backfill de E3.

## 4. Los tests propios

Además de los genéricos (`not_null`, `unique`, `accepted_values`, 27 en
total), hay 6 tests propios del dominio, en `dags/dbt/tests/`:

| Test | Qué detecta | Por qué importa |
|---|---|---|
| `assert_latitud_valida` | Latitudes fuera de [-90, 90]. | Un avión con latitud imposible es un error de la fuente o del parseo. En el mapa aparecería en cualquier lado. |
| `assert_longitud_valida` | Longitudes fuera de [-180, 180]. | Ídem. |
| `assert_altitud_valida` | Altitudes menores a -1.500 ft. | La primera versión decía "altitud no negativa" y **falló con 10 filas**. Al investigar, eran aviones en tierra (de -2 a -7 ft), que es correcto. La regla estaba mal, no los datos: la reemplazamos por un límite físico. Es el test que más nos enseñó. |
| `assert_diario_consistente` | Días donde el promedio de pilotos queda fuera de [mínimo, máximo], o sin fotos. | Si pasa, la agregación diaria está mal armada. |
| `assert_features_sin_fuga_temporal` | Que la media móvil de un día use datos posteriores a ese día. | Ver E4. |
| `assert_features_ventana_valida` | Ventanas de 7 días con más de 7 o menos de 1 día con dato. | La ventana se estaría calculando por filas y no por calendario. |

Estado: **41 nodos (8 modelos, 33 tests), todos en verde.**

### La demostración del test fallando

`dags/dbt/ci/vatsim_bronze_roto.csv` es la muestra con un avión de latitud
123,4. Con ella:

```bash
dbt build --profiles-dir . --target ci --vars '{bronze_csv: ci/vatsim_bronze_roto.csv}'
```

```
4 of 41 FAIL 1 assert_latitud_valida .................. [FAIL 1]
10 of 41 SKIP relation main.fct_aeronaves ............... [SKIP]
11 of 41 SKIP relation main.fct_posiciones .............. [SKIP]
...
```

El test lo detecta, y dbt **no construye ninguna tabla gold**: el dato malo no
llega al tablero.

## 5. Documentación y linaje

Cada modelo y cada columna tienen descripción en `models/schema.yml`, escrita
para alguien que no conoce el proyecto. El linaje se genera con
`dbt docs generate --static` y queda en un único archivo HTML que se abre en
el navegador.

> [CAPTURA E2-1] `dbt build` en el servidor, en verde.
> [CAPTURA E2-2] El grafo de linaje de `dbt docs`.
> [CAPTURA E2-3] El build con la muestra rota, con el test en rojo.
