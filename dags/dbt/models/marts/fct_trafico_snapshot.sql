with por_snapshot as (
    select
        snapshot_ts,
        count(distinct cid) as pilotos_conectados,
        count(distinct cid) filter (
            where origen is null
        ) as pilotos_sin_plan,
        count(distinct origen) as aeropuertos_origen,
        count(distinct destino) as aeropuertos_destino
    from {{ ref('stg_vatsim') }}
    group by snapshot_ts
),
con_previo as (
    select *,
        lag(pilotos_conectados) over (
            order by snapshot_ts
        ) as pilotos_previo
    from por_snapshot
)
select
    snapshot_ts,
    pilotos_conectados,
    pilotos_sin_plan,
    aeropuertos_origen,
    aeropuertos_destino,
    pilotos_previo,
    case
        when pilotos_previo is null or pilotos_previo = 0 then null
        else round(
            (pilotos_conectados - pilotos_previo)
            / cast(pilotos_previo as double) * 100,
            2
        )
    end as var_pct_pilotos
from con_previo
