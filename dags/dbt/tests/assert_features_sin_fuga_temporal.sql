-- Falla si la media movil de algun dia usa informacion posterior a ese dia.
-- Recalcula el feature a mano mirando solo los 7 dias que terminan en la
-- fecha y lo compara con el del modelo. Si alguien cambia la ventana a
-- FOLLOWING, o a una ventana centrada, este test lo detecta.
with recalculado as (
    select
        f.fecha,
        f.pilotos_media_movil_7d,
        (
            select round(avg(d.pilotos_promedio), 1)
            from {{ ref('fct_trafico_diario') }} as d
            where d.fecha between f.fecha - 6 and f.fecha
        ) as esperado
    from {{ ref('fct_features_trafico_diario') }} as f
)
select *
from recalculado
where abs(pilotos_media_movil_7d - esperado) > 0.05
