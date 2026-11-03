select *
from {{ source('lake', 'vatsim') }}
