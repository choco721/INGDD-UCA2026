select snapshot_sk, longitude
from {{ ref('stg_vatsim') }}
where longitude is not null
  and (longitude < -180 or longitude > 180)
