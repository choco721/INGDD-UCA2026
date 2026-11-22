-- Bronze: los datos crudos de VATSIM tal como se ingirieron (todo texto).
-- En produccion lee la tabla Delta del lake. En el CI, donde no hay lake,
-- lee una muestra chica con la misma forma (ver ci/ y profiles.yml).
{% if var('bronze_csv', none) %}
select *
from read_csv('{{ var("bronze_csv") }}', header = true, all_varchar = true, nullstr = 'NULL')
{% else %}
select *
from {{ source('lake', 'vatsim') }}
{% endif %}
