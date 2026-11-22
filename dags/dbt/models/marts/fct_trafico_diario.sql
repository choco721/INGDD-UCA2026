-- Gold: serie diaria del trafico de la red.
-- Grano: una fila por dia (hora de Argentina).
-- Responde: ¿cuantos pilotos vuelan por dia en VATSIM y como evoluciona?
--
-- Es el modelo que se reprocesa por intervalo en un backfill. Airflow le
-- pasa el rango de dias de la corrida (vars desde/hasta) y dbt reconstruye
-- solo esos dias: borra las filas de esas fechas e inserta las recalculadas.
-- Reprocesar un dia dos veces da el mismo resultado.
--
-- La primera construccion (o con --full-refresh) toma toda la historia de
-- bronze, sin mirar las vars.
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fecha',
    on_schema_change = 'fail'
) }}

with fotos as (
    select
        -- Argentina no tiene horario de verano: UTC-3 todo el año.
        cast(snapshot_ts - interval 3 hour as date) as fecha,
        snapshot_ts,
        pilotos_conectados,
        pilotos_sin_plan
    from {{ ref('fct_trafico_snapshot') }}
)
select
    fecha,
    count(*)                              as snapshots_del_dia,
    round(avg(pilotos_conectados), 1)     as pilotos_promedio,
    max(pilotos_conectados)               as pilotos_maximo,
    min(pilotos_conectados)               as pilotos_minimo,
    round(avg(pilotos_sin_plan), 1)       as pilotos_sin_plan_promedio,
    min(snapshot_ts)                      as primera_foto_utc,
    max(snapshot_ts)                      as ultima_foto_utc
from fotos
{% if is_incremental() %}
where fecha between cast('{{ var("desde", "1900-01-01") }}' as date)
                and cast('{{ var("hasta", "2999-12-31") }}' as date)
{% endif %}
group by fecha
