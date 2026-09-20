select bike_id, bike_type, commissioned_at, extracted_at
from {{ source('raw', 'bikes') }}
