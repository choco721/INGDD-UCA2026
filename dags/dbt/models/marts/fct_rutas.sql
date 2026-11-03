-- Gold: trafico por ruta y snapshot.
-- Grano: una fila por (snapshot_ts, origen, destino).
-- Responde cuales son las rutas y los aeropuertos mas transitados.
select
    snapshot_ts,
    origen,
    destino,
    origen || ' -> ' || destino as ruta,
    count(distinct cid)         as vuelos
from {{ ref('stg_vatsim') }}
where origen is not null
  and destino is not null
group by snapshot_ts, origen, destino
