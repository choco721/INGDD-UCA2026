-- Gold: posicion de cada vuelo en el snapshot mas reciente.
-- Grano: una fila por callsign del ultimo snapshot disponible.
-- Alimenta el mapa de Metabase y el analisis por fase de vuelo.
--
-- Se limita al ultimo snapshot a proposito: es una foto del momento, no una
-- serie historica, y asi la tabla se mantiene chica para el tablero.
--
-- No publica el nombre del piloto ni su cid: son datos personales y el
-- tablero no los necesita (ver docs/governance). El callsign es el
-- identificador publico del vuelo, el mismo que muestra cualquier radar.
with ultimo as (
    select max(snapshot_ts) as snapshot_ts
    from {{ ref('stg_vatsim') }}
)
select
    s.snapshot_ts,
    s.callsign,
    s.server,
    s.latitude,
    s.longitude,
    s.altitude_ft,
    s.groundspeed_kt,
    s.heading,
    s.origen,
    s.destino,
    s.aeronave,
    case
        when s.altitude_ft is null    then 'Sin dato'
        when s.altitude_ft < 1000     then 'En tierra'
        when s.altitude_ft < 18000    then 'Ascenso o descenso'
        else 'Crucero'
    end as fase_vuelo
from {{ ref('stg_vatsim') }} as s
inner join ultimo as u
    on s.snapshot_ts = u.snapshot_ts
