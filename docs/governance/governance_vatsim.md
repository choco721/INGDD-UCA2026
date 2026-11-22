# Documentación de governance — Dataset VATSIM

**Grupo 03 · Ingeniería de Datos · LCD · UCA Rosario · 2026**

Versión del documento: noviembre 2026

---

## 1. Identificación

| | |
|---|---|
| **Nombre** | Tráfico aéreo de la red VATSIM |
| **Qué contiene** | Fotos diarias de todos los pilotos conectados a VATSIM, una red mundial de simulación de vuelo donde personas reales vuelan aviones simulados con control de tránsito aéreo hecho por voluntarios. Para cada piloto: posición, altitud, velocidad, plan de vuelo y tipo de avión. |
| **Para qué sirve** | Medir cuánta gente vuela por día, dónde, en qué rutas y con qué aviones. Pensado para quien organiza la cobertura de controladores voluntarios. |
| **Responsable** | **Juan Cruz Chocobares.** Equipo: Agustín Formenti, Victoria Ortiz, Andrés Morenico. |
| **Dónde reclamar** | Issues del repositorio https://github.com/choco721/INGDD-UCA2026 |
| **Frecuencia** | Una foto por día, a las 18:00 (hora de Argentina). |
| **Cobertura** | Desde el 22/10/2026. Mundial. |
| **Dónde está** | Bronze: `s3://grupo03/bronze/vatsim` (Delta, MinIO). Gold publicada: esquema `grupo03` de la base `warehouse` (PostgreSQL). |

## 2. Origen y linaje

**Origen:** API pública de VATSIM, `https://data.vatsim.net/v3/vatsim-data.json`.
Sin autenticación ni cuota. Devuelve solo el estado actual de la red, que se
renueva cada ~15 segundos. No guarda historia: si un día no se ingiere, esa
foto se pierde para siempre.

**Recorrido de los datos:**

```
API VATSIM
  │  scripts/ingesta.py — tarea ingerir_bronze, diaria 18:00
  ▼
BRONZE  s3://grupo03/bronze/vatsim       Delta Lake, append-only, todo como texto
  │  dbt
  ▼
SILVER  stg_vatsim                       vista: tipado, nulos, deduplicado
  │  dbt
  ▼
GOLD    fct_trafico_snapshot ─ fct_trafico_diario ─ fct_features_trafico_diario
        fct_rutas · fct_aeronaves · fct_posiciones        (tablas en DuckDB)
  │  scripts/publicar_gold.py — tarea publicar_gold
  ▼
PostgreSQL warehouse.grupo03  ──►  Metabase
```

**Trazabilidad:**
- Cada fila de bronze tiene `_ingested_at` (cuándo entró) y `_run_id` (qué
  corrida la escribió).
- Bronze es Delta Lake: guarda el historial de versiones de la tabla, así que
  se puede ver cómo estaba en cualquier momento (*time travel*).
- Las transformaciones están versionadas en git, y `dbt docs` genera el grafo
  de linaje modelo por modelo, hasta la columna.
- Cada corrida de Airflow queda registrada con sus logs.

## 3. Diccionario de datos

Solo las tablas publicadas en PostgreSQL, que es lo que consume el tablero.
Silver y bronze están documentados columna por columna en
`dags/dbt/models/schema.yml`.

### `fct_trafico_diario` — una fila por día

| Campo | Tipo | Significado |
|---|---|---|
| `fecha` | fecha | Día, en hora de Argentina. |
| `snapshots_del_dia` | entero | Cuántas fotos se tomaron ese día. Normalmente una, a las 18:00. |
| `pilotos_promedio` | decimal | Promedio de pilotos conectados en las fotos del día. |
| `pilotos_maximo` | entero | Mayor cantidad de pilotos en una foto del día. |
| `pilotos_minimo` | entero | Menor cantidad de pilotos en una foto del día. |
| `pilotos_sin_plan_promedio` | decimal | Promedio de pilotos volando sin plan de vuelo cargado. |
| `primera_foto_utc`, `ultima_foto_utc` | fecha y hora | Hora UTC de la primera y la última foto del día. |

### `fct_features_trafico_diario` — una fila por día

| Campo | Tipo | Significado |
|---|---|---|
| `fecha` | fecha | Día al que corresponden los valores. |
| `pilotos_promedio` | decimal | Promedio de pilotos conectados ese día. |
| `pilotos_media_movil_7d` | decimal | Promedio de los últimos 7 días de calendario, incluido el día. |
| `dias_con_dato_7d` | entero | Cuántos de esos 7 días tuvieron datos (de 1 a 7). |
| `pilotos_dia_anterior` | decimal | Promedio del día anterior. Vacío si ese día no hubo datos. |
| `var_pct_vs_dia_anterior` | decimal | Cambio porcentual contra el día anterior. |
| `desvio_vs_media_7d` | decimal | Pilotos por encima (+) o por debajo (−) de la media de 7 días. |

### `fct_trafico_snapshot` — una fila por foto

| Campo | Tipo | Significado |
|---|---|---|
| `snapshot_ts` | fecha y hora | Momento de la foto, en UTC, según VATSIM. |
| `pilotos_conectados` | entero | Pilotos distintos conectados. |
| `pilotos_sin_plan` | entero | Pilotos sin plan de vuelo cargado. |
| `aeropuertos_origen` | entero | Aeropuertos distintos con al menos una salida. |
| `aeropuertos_destino` | entero | Aeropuertos distintos con al menos una llegada. |
| `pilotos_previo` | entero | Pilotos conectados en la foto anterior. |
| `var_pct_pilotos` | decimal | Cambio porcentual contra la foto anterior. |

### `fct_rutas` — una fila por foto y ruta

| Campo | Tipo | Significado |
|---|---|---|
| `snapshot_ts` | fecha y hora | Foto. |
| `origen`, `destino` | texto | Aeropuertos de salida y llegada, en código OACI (ej. SAEZ = Ezeiza). |
| `ruta` | texto | "ORIGEN -> DESTINO". |
| `vuelos` | entero | Vuelos en esa ruta en esa foto. |

### `fct_aeronaves` — una fila por foto y tipo de avión

| Campo | Tipo | Significado |
|---|---|---|
| `snapshot_ts` | fecha y hora | Foto. |
| `aeronave` | texto | Tipo de avión en código OACI (ej. A320, B738 = Boeing 737-800). |
| `vuelos` | entero | Vuelos con ese tipo de avión. |
| `altitud_media_ft` | entero | Altitud promedio de ese tipo, en pies. |
| `velocidad_media_kt` | entero | Velocidad promedio respecto del suelo, en nudos. |

### `fct_posiciones` — una fila por vuelo de la última foto

| Campo | Tipo | Significado |
|---|---|---|
| `snapshot_ts` | fecha y hora | Foto de la que sale la posición. |
| `callsign` | texto | Indicativo del vuelo (ej. AAL5644). Público: es lo que se transmite por radio. |
| `server` | texto | Servidor de VATSIM al que está conectado. |
| `latitude`, `longitude` | decimal | Posición, en grados. |
| `altitude_ft` | entero | Altitud sobre el nivel del mar, en pies. Puede ser levemente negativa en tierra. |
| `groundspeed_kt` | entero | Velocidad respecto del suelo, en nudos. |
| `heading` | entero | Rumbo, en grados. |
| `origen`, `destino`, `aeronave` | texto | Del plan de vuelo. Vacíos si el piloto no cargó plan. |
| `fase_vuelo` | texto | Estimada por altitud: *En tierra* (< 1.000 ft), *Ascenso o descenso* (< 18.000 ft), *Crucero*, o *Sin dato*. |

## 4. Clasificación de sensibilidad

**Cómo lo determinamos.** Revisamos campo por campo qué permite identificar a
una persona. La política de protección de datos de VATSIM confirma que el
nombre, el CID y el rating de cada usuario se transmiten a terceros mientras
está conectado (sección 5.2). Que un dato sea visible públicamente no le quita
el carácter de dato personal: en Argentina, la Ley 25.326 protege cualquier
información referida a personas identificadas o identificables.

| Campo | Dónde está | Clasificación | Por qué |
|---|---|---|---|
| `name` / `piloto` | bronze, silver | **Personal** | Muchas veces es nombre y apellido real. |
| `cid` | bronze, silver | **Personal** | Número de socio: identifica a una persona y permite buscar su perfil en VATSIM. |
| `pilot_rating`, `logon_time` y similares | bronze | **Personal** | Junto con el CID, describen la actividad de una persona. |
| `callsign` | todas las capas | Público | Identifica al vuelo, no a la persona. Se transmite por radio, como en la aviación real. |
| Posición, altitud, velocidad, plan de vuelo, avión | todas las capas | Público | Datos del vuelo simulado. |
| Agregados (conteos, promedios, features) | gold | Público | No permiten volver a una persona. |

**Qué hicimos con eso:**
- **Nada personal llega a gold, a PostgreSQL ni a Metabase.** Las versiones
  anteriores de `fct_posiciones` publicaban el nombre y el CID; ya no.
- No usamos un hash del CID en su lugar: los CID son números de un rango chico,
  así que un hash se revierte probando todos. No sería anónimo.
- Bronze y silver quedan con acceso solo del grupo: el bucket de MinIO es por
  grupo y exige credenciales.

**Derecho de supresión.** Si un piloto pide que borremos sus datos, se puede:

1. En bronze: `DELETE` de las filas con su `cid` en la tabla Delta.
2. `VACUUM` de la tabla, porque Delta guarda versiones anteriores y el dato
   seguiría recuperable con *time travel* hasta que se eliminen.
3. Correr el pipeline: silver es una vista y gold se recalcula desde bronze.
   Igual, gold no tenía el dato.

## 5. Calidad y garantías

**Qué garantizamos:**
- **Frescura:** un dato nuevo por día, a las 18:00. El tablero muestra "horas
  desde el último dato": más de 24 significa que el pipeline no corrió.
- **Sin duplicados:** una fila por piloto y foto, garantizado por la ingesta
  idempotente y testeado en silver.
- **Rangos físicos válidos:** latitud, longitud y altitud dentro de rangos
  posibles.
- **Nada se publica sin pasar los tests:** 33 tests corren en cada ejecución;
  si uno falla, la gold no se publica y el tablero conserva el último dato
  válido.

**Huecos y limitaciones conocidas:**

| Hueco o limitación | Detalle |
|---|---|
| **No hay datos antes del 22/10/2026** | VATSIM no guarda historia. No se puede recuperar. |
| **Frecuencia irregular hasta el 3/11** | Mientras se construía el pipeline las corridas fueron manuales: hay días con varias fotos y días sin ninguna. `snapshots_del_dia` lo muestra. |
| **Gold desactualizada del 6 al 9/11** | La ingesta corrió todos esos días, pero dbt falló por un archivo de respaldo del editor de Jupyter. Bronze está completo; la gold se puso al día el 10/11. |
| **Una foto por día no es el total del día** | `pilotos_promedio` mide a las 18:00 de Argentina, no los pilotos que volaron en todo el día. El tráfico cambia mucho según la hora. |
| **Ruta GVAC → DNMM sospechosa** | El 3/11, 25 vuelos en esa ruta (Cabo Verde → Lagos), varios quietos en el mismo punto exacto del aeropuerto de Sal. Puede ser un evento, pilotos ausentes o un punto de aparición por defecto. No se filtró, porque no sabemos la causa. |
| **El plan de vuelo lo carga el piloto** | Origen, destino y avión son texto libre del usuario: puede haber códigos mal escritos. |
| **`fase_vuelo` es una estimación** | Sale solo de la altitud. Un avión en crucero bajo aparece como "ascenso o descenso". |
| **Se asume que el callsign es único** | VATSIM no admite dos conectados con el mismo; un test lo vigila. |

**Lo que ningún test detecta:** que el volumen caiga mucho de un día a otro sin
error (los tests miden forma, no cantidad), que una regla de negocio esté mal
pensada (como pasó con la altitud negativa) o que la API cambie el significado
de un campo sin cambiar su nombre.

## 6. Licencia y uso

**Del origen.** VATSIM no publica una licencia explícita para su feed público
de datos. Su política de protección de datos indica que nombre, CID y rating se
transmiten a otras organizaciones mientras el usuario está conectado (sección
5.2) y reconoce a cada usuario el derecho a pedir el borrado de sus datos
(sección 13).
Fuente: [VATSIM Data Protection and Handling Policy v1.4](https://cdn.vatsim.net/policy-documents/Data%20Protection%20and%20Handling%20Policy%20v1.4.pdf).

**De este dataset:**
- **Uso permitido:** académico, dentro de la materia. Análisis agregado del
  tráfico de la red.
- **No permitido:** uso comercial; republicar datos de personas (nombre, CID);
  cruzar el dataset con otras fuentes para identificar pilotos.
- **Para cualquier otro uso:** consultar primero a VATSIM, porque no hay
  licencia que lo autorice.
- **Cita sugerida:** "Datos de VATSIM (data.vatsim.net), procesados por el
  grupo 03 de Ingeniería de Datos, LCD UCA Rosario, 2026."
