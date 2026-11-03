select snapshot_sk, latitude
from {{ ref('stg_vatsim') }}
where latitude is not null
  and (latitude < -90 or latitude > 90)
