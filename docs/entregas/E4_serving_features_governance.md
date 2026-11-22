# Entrega 4 — Serving, features y governance

**Grupo 03 · Ingeniería de Datos · LCD · UCA Rosario · 2026**

Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés

Repositorio: https://github.com/choco721/INGDD-UCA2026

---

Con esta entrega los datos hacen el recorrido completo:

```
API VATSIM → ingesta → bronze (Delta, MinIO) → dbt: silver → gold (DuckDB)
          → publicar_gold → PostgreSQL warehouse.grupo03 → Metabase
```

## 1. La gold en PostgreSQL

**Qué se publica.** Las seis tablas gold, en el esquema `grupo03` de la base
`warehouse`: `fct_trafico_diario`, `fct_features_trafico_diario`,
`fct_trafico_snapshot`, `fct_rutas`, `fct_aeronaves` y `fct_posiciones`.

**Cómo, y dónde está en el pipeline.** Es la quinta tarea del DAG,
`publicar_gold` (`dags/scripts/publicar_gold.py`), y corre solo si los tests
de dbt pasaron. Nadie aprieta ningún botón: el pipeline termina en el
warehouse.

**Es repetible: correrla dos veces deja el mismo resultado.** Cada tabla se
reemplaza entera (`if_exists="replace"`), así que no hay forma de acumular
filas repetidas. Además, las seis tablas se escriben en **una sola transacción**:
si algo falla a mitad de camino, PostgreSQL revierte todo y el tablero sigue
mostrando la publicación anterior completa, nunca una mitad. Antes de escribir,
el script verifica que la conexión apunte a la base y el usuario del grupo.
Después de escribir, compara las filas leídas con las publicadas.

**Por qué la gold se mueve del lakehouse al warehouse**, y Metabase no lee
directo de DuckDB o del lake:

- *Concurrencia.* `warehouse.duckdb` es un archivo con un solo escritor. Si
  Metabase lo tuviera abierto mientras Airflow corre dbt, uno de los dos
  quedaría bloqueado. PostgreSQL atiende muchas lecturas mientras se escribe.
- *Separar quién construye de quién consume.* El tablero lee una copia estable
  y ya validada. Una corrida que falla en los tests no llega a PostgreSQL.
- *Acceso y permisos.* PostgreSQL da a cada grupo su esquema y su usuario. El
  lake tiene credenciales de escritura que un tablero no debería tener.
- *Velocidad.* Metabase consulta tablas chicas y agregadas, sin ir a MinIO ni
  leer archivos Delta en cada gráfico.

**Consulta desde afuera de Python:** desde *Nuevo → Consulta SQL* en Metabase,
o con `psql`:

```sql
select fecha, pilotos_promedio, pilotos_media_movil_7d
from grupo03.fct_features_trafico_diario
order by fecha desc
limit 7;
```

> [CAPTURA E4-1] Esa consulta corriendo en Metabase o psql.
> [CAPTURA E4-2] Log de `publicar_gold` con las seis tablas publicadas.

## 2. El tablero

Tablero **"VATSIM · Tráfico aéreo en tiempo real"**, colección *Grupo03 ·
VATSIM* de Metabase. Las preguntas, el SQL de cada gráfico y las capturas están
en `docs/tablero/preguntas_tablero.md`.

| Gráfico | Pregunta que responde | Qué decisión ayuda a tomar |
|---|---|---|
| Horas desde el último dato | ¿Los datos están al día? | Si confiar en el resto del tablero. Más de 24 h = el pipeline no corrió. |
| Pilotos por día y media de 7 días | ¿Cuánta gente vuela por día y está creciendo o cayendo? | Qué días convocar más controladores voluntarios de tránsito aéreo. |
| Posición actual de los vuelos | ¿Dónde está volando la gente ahora? | Qué regiones necesitan controlador en este momento. |
| Top 10 rutas | ¿Qué rutas concentran más vuelos? | Qué pares de aeropuertos priorizar en un evento. |
| Mix de flota | ¿Qué aviones se vuelan más? | Qué aeropuertos preparar para aviones grandes (posiciones, pistas). |
| Fase de vuelo | ¿Cuántos están en tierra, subiendo o bajando, o en crucero? | Cuánta carga hay en torres de control frente a control en ruta. |

El tablero carga rápido porque cada gráfico lee una tabla ya agregada por dbt:
Metabase no recorre las decenas de miles de filas de silver en cada consulta.

> El tablero no se versiona: vive en Metabase, no en el repositorio. Para la
> defensa hay que rehacerlo en el servidor a partir del SQL guardado en
> `docs/tablero/preguntas_tablero.md`.

## 3. Los features

**Dónde están:** `fct_features_trafico_diario`, un modelo dbt en gold
(`dags/dbt/models/marts/`). Es el único lugar donde se definen: versionados en
git, con tests, y publicados con el resto de la gold.

**Para qué:** un modelo que intentara predecir el tráfico de mañana usaría
estas variables. No entrenamos ningún modelo; el objetivo es que los features
estén bien calculados y sean reproducibles.

| Feature | Definición |
|---|---|
| `pilotos_promedio` | Promedio de pilotos conectados en las fotos del día (variable base). |
| `pilotos_media_movil_7d` | Promedio de `pilotos_promedio` de los últimos 7 días **de calendario**, incluido el día. |
| `dias_con_dato_7d` | Cuántos de esos 7 días tuvieron datos. |
| `pilotos_dia_anterior` | `pilotos_promedio` del día exactamente anterior; nulo si ese día no hubo datos. |
| `var_pct_vs_dia_anterior` | Variación porcentual contra el día anterior. |
| `desvio_vs_media_7d` | Diferencia entre el día y su media de 7 días. |

**Reproducibles.** Se calculan desde bronze con SQL determinístico. Borrar la
tabla y recalcularla da el mismo resultado; reprocesar un día también.

**Sin fuga temporal.** Para calcular el feature del día 15, ¿se usó algún dato
que el día 15 todavía no existía? No: todas las ventanas miran solo hacia
atrás (`RANGE BETWEEN ... PRECEDING AND CURRENT ROW`), así que el feature del
15 usa datos hasta el cierre del 15, que es cuando se calcularía para predecir
el 16. Una "media del mes" calculada para el día 15 con el mes completo
incluiría días 16 a 30: eso sería una fuga.

**Ventanas por calendario, no por filas.** El pipeline estuvo caído del 6 al
9/11. Una ventana de "las últimas 7 filas" habría cubierto 11 días sin
avisar. Con `RANGE` sobre la fecha, la ventana es siempre de 7 días, y
`dias_con_dato_7d` dice cuántos tuvieron dato. Por la misma razón,
`pilotos_dia_anterior` es nulo cuando falta el día anterior, en vez de tomar
"el último día disponible", que podría ser de hace una semana.

**Tests:**
- `assert_features_sin_fuga_temporal` recalcula la media móvil de cada día
  mirando solo los 7 días que terminan en esa fecha, y falla si no coincide
  con la del modelo. Si alguien cambiara la ventana a `FOLLOWING`, o a una
  ventana centrada, el test lo detectaría.
- `assert_features_ventana_valida`: `dias_con_dato_7d` entre 1 y 7.
- `unique` y `not_null` en `fecha`; `not_null` en los features base.

> [CAPTURA E4-3] La tabla de features en Metabase.

## 4. Governance

El documento completo, con sus seis secciones, está en
`docs/governance/governance_vatsim.md`. Lo central:

- **Responsable:** Juan Cruz Chocobares, con el resto del grupo 03.
- **Hay datos personales en el origen.** VATSIM publica el nombre y el número
  de socio (CID) de cada piloto conectado, y muchas veces el nombre es real.
  Decidimos que **ningún dato personal llegue a gold ni al tablero**: quedan en
  bronze y silver, con acceso solo del grupo. `fct_posiciones` identifica cada
  vuelo por su indicativo (callsign), que es público y se transmite por radio.
- **Si un piloto pide que borremos sus datos, se puede**: el procedimiento está
  documentado (borrado en Delta, eliminación de versiones viejas y
  reconstrucción).
- **Huecos conocidos**, documentados: datos desde el 22/10; frecuencia
  irregular hasta el 3/11; gold desactualizada del 6 al 9/11; una foto por día
  no es el total del día.

> [CAPTURA E4-4] El diccionario de datos del documento de governance.
