-- Una ventana de 7 dias tiene entre 1 y 7 dias con dato. Fuera de ese rango,
-- la ventana se esta calculando mal (por filas en vez de por calendario, o
-- con duplicados en la serie diaria).
select fecha, dias_con_dato_7d
from {{ ref('fct_features_trafico_diario') }}
where dias_con_dato_7d not between 1 and 7
