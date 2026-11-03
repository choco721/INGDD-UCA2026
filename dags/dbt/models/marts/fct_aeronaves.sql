-- Gold: composicion de la flota por snapshot.
-- Grano: una fila por (snapshot_ts, aeronave).
-- Muestra que tipos de avion predominan y como cambia el mix entre fotos.
select
    snapshot_ts,
    aeronave,
    count(distinct cid)              as vuelos,
    round(avg(altitude_ft), 0)       as altitud_media_ft,
    round(avg(groundspeed_kt), 0)    as velocidad_media_kt
from {{ ref('stg_vatsim') }}
where aeronave is not null
group by snapshot_ts, aeronave
