select snapshot_sk, altitude_ft
from {{ ref('stg_vatsim') }}
where altitude_ft is not null
  and altitude_ft < -1500
