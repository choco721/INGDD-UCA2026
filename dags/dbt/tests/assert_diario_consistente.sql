-- El promedio diario tiene que quedar entre el minimo y el maximo del dia, y
-- cada dia tiene al menos una foto. Si no, la agregacion esta mal armada.
select fecha, pilotos_minimo, pilotos_promedio, pilotos_maximo, snapshots_del_dia
from {{ ref('fct_trafico_diario') }}
where snapshots_del_dia < 1
   or pilotos_promedio < pilotos_minimo
   or pilotos_promedio > pilotos_maximo
