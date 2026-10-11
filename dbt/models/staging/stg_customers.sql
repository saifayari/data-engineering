select
    customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
