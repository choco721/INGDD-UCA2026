# Tablero "VATSIM · Tráfico aéreo en tiempo real"

Metabase, colección **Grupo03 · VATSIM**, base **warehouse**, esquema `grupo03`.

El tablero no se versiona: vive en Metabase. Este archivo guarda lo necesario
para rehacerlo desde cero en cualquier servidor: la pregunta de cada gráfico,
su SQL y cómo se configura. Las capturas están en `capturas/`.

**Cómo crear cada gráfico:** *+ Nuevo → Consulta SQL* → base **warehouse** →
pegar el SQL → ▶ → elegir la visualización → *Guardar* en la colección.

**Orden en el tablero:** frescura arriba, chica. Debajo, la serie diaria a lo
ancho. Después el mapa a lo ancho. Abajo, rutas y flota lado a lado, y fase y
control de publicación lado a lado.

---

## 1. Horas desde el último dato

**Pregunta:** ¿los datos del tablero están al día?
**Decisión:** si confiar en el resto. Más de 24 significa que el pipeline no corrió.
**Visualización:** Número, con sufijo " h".

```sql
select round(extract(epoch from
         (now() at time zone 'UTC') - max(snapshot_ts)) / 3600) as horas_desde_ultimo_dato
from grupo03.fct_trafico_snapshot
```

## 2. Pilotos por día y media de 7 días

**Pregunta:** ¿cuánta gente vuela por día en la red, y el tráfico está
creciendo o cayendo?
**Decisión:** qué días hace falta convocar más controladores voluntarios.
**Visualización:** Línea, con las dos series. Eje X: `Día`.

```sql
select
    fecha                   as "Día",
    pilotos_promedio        as "Pilotos conectados (18 h)",
    pilotos_media_movil_7d  as "Media de los últimos 7 días"
from grupo03.fct_features_trafico_diario
order by fecha
```

## 3. Posición actual de los vuelos

**Pregunta:** ¿dónde está volando la gente ahora mismo?
**Decisión:** qué regiones necesitan controlador en este momento.
**Visualización:** Mapa → *Mapa de pines*. Latitud: `latitude`, longitud: `longitude`.

```sql
select callsign, latitude, longitude, altitude_ft, origen, destino, aeronave, fase_vuelo
from grupo03.fct_posiciones
```

## 4. Top 10 rutas

**Pregunta:** ¿qué pares de aeropuertos concentran más vuelos?
**Decisión:** qué rutas priorizar al organizar un evento.
**Visualización:** Barras.

```sql
select ruta as "Ruta", sum(vuelos) as "Vuelos"
from grupo03.fct_rutas
where snapshot_ts = (select max(snapshot_ts) from grupo03.fct_rutas)
group by ruta
order by 2 desc
limit 10
```

## 5. Mix de flota

**Pregunta:** ¿qué aviones se vuelan más en la red?
**Decisión:** qué aeropuertos preparar para aviones grandes.
**Visualización:** Circular. La cola de tipos poco frecuentes se agrupa en
"Otros" para que el gráfico se pueda leer.

```sql
with ultimo as (
    select max(snapshot_ts) as snapshot_ts from grupo03.fct_aeronaves
),
ranking as (
    select a.aeronave, a.vuelos,
           row_number() over (order by a.vuelos desc) as puesto
    from grupo03.fct_aeronaves as a
    inner join ultimo as u on a.snapshot_ts = u.snapshot_ts
),
agrupado as (
    select case when puesto <= 10 then aeronave else 'Otros' end as aeronave,
           sum(vuelos) as vuelos
    from ranking
    group by 1
)
select aeronave as "Avión", vuelos as "Vuelos"
from agrupado
order by case when aeronave = 'Otros' then 1 else 0 end, vuelos desc
```

## 6. Fase de vuelo

**Pregunta:** ¿cuántos vuelos están en tierra, subiendo o bajando, o en crucero?
**Decisión:** cuánta carga hay en torres de control frente al control en ruta.
**Visualización:** Circular.

```sql
select fase_vuelo as "Fase", count(*) as "Vuelos"
from grupo03.fct_posiciones
group by fase_vuelo
order by 2 desc
```

## 7. Datos publicados

**Pregunta:** ¿llegaron todas las tablas en la última publicación?
**Decisión:** control operativo: si una tabla falta o está vacía, la
publicación falló.
**Visualización:** Tabla.

```sql
select 'fct_trafico_diario' as tabla, count(*) as filas from grupo03.fct_trafico_diario
union all select 'fct_features_trafico_diario', count(*) from grupo03.fct_features_trafico_diario
union all select 'fct_trafico_snapshot', count(*) from grupo03.fct_trafico_snapshot
union all select 'fct_rutas', count(*) from grupo03.fct_rutas
union all select 'fct_aeronaves', count(*) from grupo03.fct_aeronaves
union all select 'fct_posiciones', count(*) from grupo03.fct_posiciones
```
