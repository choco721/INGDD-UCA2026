with base as (
    select
        cast(cid as bigint)                   as cid,
        cast(callsign as varchar)             as callsign,
        cast(name as varchar)                 as piloto,
        cast(server as varchar)               as server,
        cast(latitude as double)              as latitude,
        cast(longitude as double)             as longitude,
        cast(altitude as integer)             as altitude_ft,
        cast(groundspeed as integer)          as groundspeed_kt,
        cast(heading as integer)              as heading,
        nullif(flight_plan_departure, '')     as origen,
        nullif(flight_plan_arrival, '')       as destino,
        nullif(flight_plan_aircraft_short, '') as aeronave,
        cast(snapshot_ts as timestamp)        as snapshot_ts
    from {{ ref('brz_vatsim') }}
    where cid is not null
      and snapshot_ts is not null
),
dedup as (
    select *,
        row_number() over (
            partition by cid, snapshot_ts
            order by snapshot_ts
        ) as rn
    from base
)
select
    cast(cid as varchar) || '-' ||
        cast(snapshot_ts as varchar) as snapshot_sk,
    cid,
    callsign,
    piloto,
    server,
    latitude,
    longitude,
    altitude_ft,
    groundspeed_kt,
    heading,
    origen,
    destino,
    aeronave,
    snapshot_ts
from dedup
where rn = 1
