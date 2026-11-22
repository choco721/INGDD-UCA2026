-- Gold: features diarios del trafico, definidos una sola vez.
-- Grano: una fila por dia.
-- Cada feature usa solo informacion disponible al cierre de su dia: todas
-- las ventanas miran hacia atras (PRECEDING), nunca hacia adelante.
-- Las ventanas son por calendario (RANGE), no por cantidad de filas: si el
-- pipeline estuvo caido unos dias, la ventana de 7 dias sigue siendo de 7
-- dias y dias_con_dato_7d avisa cuantos tuvieron datos.
with diario as (
    select fecha, pilotos_promedio
    from {{ ref('fct_trafico_diario') }}
),
ventanas as (
    select
        fecha,
        pilotos_promedio,
        avg(pilotos_promedio) over ultimos_7_dias   as pilotos_media_movil_7d,
        count(*)              over ultimos_7_dias   as dias_con_dato_7d,
        max(pilotos_promedio) over (
            order by fecha
            range between interval 1 day preceding and interval 1 day preceding
        )                                           as pilotos_dia_anterior
    from diario
    window ultimos_7_dias as (
        order by fecha
        range between interval 6 day preceding and current row
    )
)
select
    fecha,
    pilotos_promedio,
    round(pilotos_media_movil_7d, 1)                            as pilotos_media_movil_7d,
    dias_con_dato_7d,
    pilotos_dia_anterior,
    round((pilotos_promedio - pilotos_dia_anterior)
          / nullif(pilotos_dia_anterior, 0) * 100, 2)           as var_pct_vs_dia_anterior,
    round(pilotos_promedio - pilotos_media_movil_7d, 1)         as desvio_vs_media_7d
from ventanas
